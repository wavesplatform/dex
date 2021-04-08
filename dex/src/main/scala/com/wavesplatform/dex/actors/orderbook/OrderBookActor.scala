package com.wavesplatform.dex.actors.orderbook

import akka.actor.typed
import akka.actor.typed.scaladsl.adapter._
import akka.{actor => classic}
import cats.data.NonEmptyList
import cats.instances.option.catsStdInstancesForOption
import cats.syntax.apply._
import cats.syntax.option._
import com.wavesplatform.dex.actors.MatcherActor.{ForceStartOrderBook, OrderBookCreated, SaveSnapshot}
import com.wavesplatform.dex.actors.address.AddressActor
import com.wavesplatform.dex.actors.events.OrderEventsCoordinatorActor
import com.wavesplatform.dex.actors.orderbook.OrderBookActor._
import com.wavesplatform.dex.actors.{MatcherActor, WorkingStash, orderbook}
import com.wavesplatform.dex.api.ws.actors.WsInternalBroadcastActor
import com.wavesplatform.dex.api.ws.protocol.WsOrdersUpdate
import com.wavesplatform.dex.domain.asset.AssetPair
import com.wavesplatform.dex.domain.utils.{LoggerFacade, ScorexLogging}
import com.wavesplatform.dex.error
import com.wavesplatform.dex.error.ErrorFormatterContext
import com.wavesplatform.dex.metrics.TimerExt
import com.wavesplatform.dex.model.Events._
import com.wavesplatform.dex.model.OrderBook.OrderBookUpdates
import com.wavesplatform.dex.model.{LastTrade, _}
import com.wavesplatform.dex.queue.{ValidatedCommand, ValidatedCommandWithMeta}
import com.wavesplatform.dex.settings.{DenormalizedMatchingRule, MatchingRule, OrderRestrictionsSettings}
import com.wavesplatform.dex.time.Time
import kamon.Kamon
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

class OrderBookActor(
  settings: Settings,
  owner: classic.ActorRef,
  eventsCoordinatorRef: typed.ActorRef[OrderEventsCoordinatorActor.Message],
  snapshotStore: classic.ActorRef,
  wsInternalHandlerDirectoryRef: typed.ActorRef[WsInternalBroadcastActor.Command],
  assetPair: AssetPair,
  time: Time,
  var matchingRules: NonEmptyList[DenormalizedMatchingRule],
  updateCurrentMatchingRules: DenormalizedMatchingRule => Unit,
  normalizeMatchingRule: DenormalizedMatchingRule => MatchingRule,
  getMakerTakerFeeByOffset: Long => (AcceptedOrder, LimitOrder) => (Long, Long),
  restrictions: Option[OrderRestrictionsSettings]
)(implicit ec: ExecutionContext, efc: ErrorFormatterContext)
    extends classic.Actor
    with WorkingStash
    with ScorexLogging {

  override protected lazy val log = LoggerFacade(LoggerFactory.getLogger(s"OrderBookActor[$assetPair]"))

  private var aggregatedRef: typed.ActorRef[AggregatedOrderBookActor.Message] = _

  private var savingSnapshot = Option.empty[ValidatedCommandWithMeta.Offset]
  private var lastSavedSnapshotOffset = Option.empty[ValidatedCommandWithMeta.Offset]
  private var lastProcessedOffset = Option.empty[ValidatedCommandWithMeta.Offset]

  private val addTimer = Kamon.timer("matcher.orderbook.add").withTag("pair", assetPair.toString)
  private val cancelTimer = Kamon.timer("matcher.orderbook.cancel").withTag("pair", assetPair.toString)
  private var orderBook = OrderBook.empty

  private var actualRule: MatchingRule = normalizeMatchingRule(matchingRules.head)

  private def actualizeRules(offset: ValidatedCommandWithMeta.Offset): Unit = {
    val actualRules = DenormalizedMatchingRule.skipOutdated(offset, matchingRules)
    if (matchingRules.head != actualRules.head) {
      matchingRules = actualRules
      updateCurrentMatchingRules(matchingRules.head)
      actualRule = normalizeMatchingRule(matchingRules.head)
      // Could be unset during the start
      Option(aggregatedRef).foreach {
        _ ! AggregatedOrderBookActor.Command.ApplyChanges(
          LevelAmounts.empty,
          None,
          Some(matchingRules.head.tickSize.toDouble),
          System.currentTimeMillis
        )
      }
    }
  }

  override def receive: Receive = recovering

  private def recovering: Receive = {
    case OrderBookSnapshotStoreActor.Response.GetSnapshot(result) =>
      result.foreach { case (_, snapshot) => orderBook = OrderBook(snapshot) }

      lastSavedSnapshotOffset = result.map(_._1)
      lastProcessedOffset = lastSavedSnapshotOffset

      log.debug(
        lastSavedSnapshotOffset match {
          case None => "Recovery completed"
          case Some(x) => s"Recovery completed at $x: $orderBook"
        }
      )

      lastProcessedOffset foreach actualizeRules

      aggregatedRef = context.spawn(
        orderbook.AggregatedOrderBookActor(
          settings.aggregated,
          assetPair,
          efc.unsafeAssetDecimals(assetPair.amountAsset),
          efc.unsafeAssetDecimals(assetPair.priceAsset),
          restrictions,
          matchingRules.head.tickSize.toDouble,
          time,
          AggregatedOrderBookActor.State.fromOrderBook(orderBook)
        ),
        "aggregated"
      )
      context.watch(aggregatedRef)

      // Timestamp here doesn't matter
      processEvents(time.getTimestamp(), orderBook.allOrders.map(lo => OrderAdded(lo, OrderAddedReason.OrderBookRecovered, lo.order.timestamp)))

      owner ! OrderBookRecovered(assetPair, lastSavedSnapshotOffset)
      context.become(working)
      unstashAll()

    case x => stash(x)
  }

  private def working: Receive = {
    case request: ValidatedCommandWithMeta =>
      actualizeRules(request.offset)
      lastProcessedOffset match {
        case Some(lastProcessed) if request.offset <= lastProcessed => // Already processed
        case _ =>
          lastProcessedOffset = Some(request.offset)
          request.command match {
            case ValidatedCommand.PlaceOrder(limitOrder) => onAddOrder(request, limitOrder)
            case ValidatedCommand.PlaceMarketOrder(marketOrder) => onAddOrder(request, marketOrder)
            case x: ValidatedCommand.CancelOrder => onCancelOrder(request, x)
            case _: ValidatedCommand.DeleteOrderBook =>
              process(request.timestamp, orderBook.cancelAll(request.timestamp, OrderCanceledReason.OrderBookDeleted))
              // We don't delete the snapshot, because it could be required after restart
              // snapshotStore ! OrderBookSnapshotStoreActor.Message.Delete(assetPair)
              aggregatedRef ! AggregatedOrderBookActor.Event.OrderBookRemoved
              context.stop(self)
              log.info("SLEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEP")
              Thread.sleep(5000)
          }
      }

    case MatcherActor.Ping => sender() ! MatcherActor.Pong

    case ForceStartOrderBook(p) if p == assetPair => sender() ! OrderBookCreated(assetPair)

    case OrderBookSnapshotStoreActor.Response.Updated(offset) =>
      log.info(s"Snapshot has been saved at offset $offset")
      lastSavedSnapshotOffset = Some(offset)
      owner ! OrderBookSnapshotUpdateCompleted(assetPair, lastSavedSnapshotOffset)
      savingSnapshot = None

    case SaveSnapshot(globalEventNr) =>
      if (savingSnapshot.isEmpty && lastSavedSnapshotOffset.getOrElse(-1L) < globalEventNr) {
        saveSnapshotAt(globalEventNr)
        savingSnapshot = Some(globalEventNr)
      }

    case x: AggregatedOrderBookActor.Message => aggregatedRef.tell(x)

    case classic.Terminated(ref) =>
      log.error(s"Terminated actor: $ref")
      // If this happens the issue is critical and should not be handled. The order book will be stopped, see MatcherActor
      if (ref == aggregatedRef) throw new RuntimeException("Aggregated order book was terminated")
  }

  private def process(timestamp: Long, result: OrderBookUpdates): Unit = {
    orderBook = result.orderBook
    aggregatedRef ! AggregatedOrderBookActor.Command.ApplyChanges(result.levelChanges, result.lastTrade, None, timestamp)
    processEvents(timestamp, result.events)
  }

  private def processEvents(timestamp: Long, events: IterableOnce[Event]): Unit =
    events.iterator.foreach { event =>
      logEvent(event)
      eventsCoordinatorRef ! OrderEventsCoordinatorActor.Command.Process(event)

      val changes = event match {
        case event: Events.OrderExecuted => WsOrdersUpdate.from(event, timestamp).some
        case event: Events.OrderCanceled => WsOrdersUpdate.from(event).some
        case _ => none
      }
      changes.map(WsInternalBroadcastActor.Command.Collect).foreach(wsInternalHandlerDirectoryRef ! _)
    }

  private def logEvent(e: Event): Unit = log.info {
    import Events._
    e match {
      case e: OrderAdded => s"OrderAdded(${e.order.order.id()}, amount=${e.order.amount})"
      case e: OrderCanceled => s"OrderCanceled(${e.acceptedOrder.order.id()}, ${e.reason})"
      case e: OrderExecuted =>
        s"OrderExecuted(s=${e.submitted.order.id()}, c=${e.counter.order.id()}, amount=${e.executedAmount}, ts=${e.timestamp})"
    }
  }

  private def onCancelOrder(command: ValidatedCommandWithMeta, cancelCommand: ValidatedCommand.CancelOrder): Unit = cancelTimer.measure {
    orderBook.cancel(cancelCommand.orderId, toReason(cancelCommand.source), command.timestamp) match {
      case (updatedOrderBook, Some(cancelEvent), levelChanges) =>
        // TODO replace by process() in Scala 2.13
        orderBook = updatedOrderBook
        aggregatedRef ! AggregatedOrderBookActor.Command.ApplyChanges(levelChanges, None, None, cancelEvent.timestamp)
        processEvents(cancelEvent.timestamp, List(cancelEvent))
      case _ =>
        log.warn(s"Can't apply $command: order not found")
        eventsCoordinatorRef ! OrderEventsCoordinatorActor.Command.ProcessError(
          OrderCancelFailed(cancelCommand.orderId, error.OrderNotFound(cancelCommand.orderId))
        )
    }
  }

  private def toReason(source: AddressActor.Command.Source): OrderCanceledReason = {
    import AddressActor.Command.Source
    source match {
      case Source.NotTracked => Events.NotTracked
      case Source.Request => OrderCanceledReason.RequestExecuted
      case Source.Expiration => OrderCanceledReason.Expired
      case Source.BalanceTracking => OrderCanceledReason.InsufficientBalance
    }
  }

  private def onAddOrder(command: ValidatedCommandWithMeta, acceptedOrder: AcceptedOrder): Unit = addTimer.measure {
    log.trace(s"Applied $command, trying to match ...")
    process(
      command.timestamp,
      orderBook.add(acceptedOrder, command.timestamp, getMakerTakerFeeByOffset(command.offset), actualRule.tickSize)
    )
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.warn(s"Restarting actor because of $message", reason)
    super.preRestart(reason, message)
  }

  private def saveSnapshotAt(globalEventNr: ValidatedCommandWithMeta.Offset): Unit = {
    val saveSnapshot = (lastSavedSnapshotOffset, lastProcessedOffset).tupled.forall { case (saved, processed) => saved < processed }
    val toSave = if (saveSnapshot) Some(orderBook.snapshot) else None

    if (saveSnapshot) {
      log.trace(s"About to save snapshot $orderBook")
      log.debug(
        s"Saving both offset and snapshot. Global seqNr=$globalEventNr, local seqNr=$lastProcessedOffset, current offset = $lastSavedSnapshotOffset"
      )
    } else
      log.debug(s"Saving offset only. Global seqNr=$globalEventNr, local seqNr=$lastProcessedOffset, current offset = $lastSavedSnapshotOffset")

    snapshotStore ! OrderBookSnapshotStoreActor.Message.Update(assetPair, globalEventNr, toSave)
  }

  snapshotStore ! OrderBookSnapshotStoreActor.Message.GetSnapshot(assetPair)
}

object OrderBookActor {

  case class Settings(aggregated: AggregatedOrderBookActor.Settings)

  def props(
    settings: Settings,
    parent: classic.ActorRef,
    eventsCoordinatorRef: typed.ActorRef[OrderEventsCoordinatorActor.Message],
    snapshotStore: classic.ActorRef,
    wsInternalHandlerDirectoryRef: typed.ActorRef[WsInternalBroadcastActor.Command],
    assetPair: AssetPair,
    time: Time,
    matchingRules: NonEmptyList[DenormalizedMatchingRule],
    updateCurrentMatchingRules: DenormalizedMatchingRule => Unit,
    normalizeMatchingRule: DenormalizedMatchingRule => MatchingRule,
    getMakerTakerFeeByOffset: Long => (AcceptedOrder, LimitOrder) => (Long, Long),
    restrictions: Option[OrderRestrictionsSettings]
  )(implicit ec: ExecutionContext, efc: ErrorFormatterContext): classic.Props =
    classic.Props(
      new OrderBookActor(
        settings,
        parent,
        eventsCoordinatorRef,
        snapshotStore,
        wsInternalHandlerDirectoryRef,
        assetPair,
        time,
        matchingRules,
        updateCurrentMatchingRules,
        normalizeMatchingRule,
        getMakerTakerFeeByOffset,
        restrictions
      )
    )

  def name(assetPair: AssetPair): String = assetPair.toString

  case class MarketStatus(
    lastTrade: Option[LastTrade],
    bestBid: Option[LevelAgg],
    bestAsk: Option[LevelAgg]
  )

  object MarketStatus {
    def apply(ob: OrderBook): MarketStatus = MarketStatus(ob.lastTrade, ob.bestBid, ob.bestAsk)
  }

  case class Snapshot(eventNr: Option[Long], orderBook: OrderBookSnapshot)

  // Internal messages
  case class OrderBookRecovered(assetPair: AssetPair, eventNr: Option[Long])
  case class OrderBookSnapshotUpdateCompleted(assetPair: AssetPair, currentOffset: Option[Long])
  case object SendWsUpdates
}
