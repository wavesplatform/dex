package com.wavesplatform.dex.grpc.integration.clients.combined

import cats.instances.future._
import cats.instances.set._
import cats.syntax.contravariantSemigroupal._
import cats.syntax.group._
import cats.syntax.option._
import com.google.protobuf.ByteString
import com.wavesplatform.dex.domain.account.{Address, PublicKey}
import com.wavesplatform.dex.domain.asset.Asset
import com.wavesplatform.dex.domain.asset.Asset.IssuedAsset
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.order.Order
import com.wavesplatform.dex.domain.transaction.ExchangeTransaction
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.grpc.integration.clients.blockchainupdates.BlockchainUpdatesClient
import com.wavesplatform.dex.grpc.integration.clients.combined.CombinedWavesBlockchainClient._
import com.wavesplatform.dex.grpc.integration.clients.domain.StatusUpdate.LastBlockHeight
import com.wavesplatform.dex.grpc.integration.clients.domain._
import com.wavesplatform.dex.grpc.integration.clients.domain.portfolio.SynchronizedPessimisticPortfolios
import com.wavesplatform.dex.grpc.integration.clients.matcherext.MatcherExtensionClient
import com.wavesplatform.dex.grpc.integration.clients.{BroadcastResult, CheckedBroadcastResult, RunScriptResult, WavesBlockchainClient}
import com.wavesplatform.dex.grpc.integration.dto.BriefAssetDescription
import com.wavesplatform.dex.grpc.integration.protobuf.DexToPbConversions._
import com.wavesplatform.dex.grpc.integration.protobuf.PbToDexConversions._
import com.wavesplatform.dex.grpc.integration.services.UtxTransaction
import com.wavesplatform.protobuf.transaction.SignedTransaction
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._
import scala.util.{Failure, Success}

class CombinedWavesBlockchainClient(
  settings: Settings,
  matcherPublicKey: PublicKey,
  meClient: MatcherExtensionClient,
  bClient: BlockchainUpdatesClient
)(implicit ec: ExecutionContext, monixScheduler: Scheduler)
    extends WavesBlockchainClient
    with ScorexLogging {

  type Balances = Map[Address, Map[Asset, Long]]
  type Leases = Map[Address, Long]

  private val pbMatcherPublicKey = matcherPublicKey.toPB

  private val pessimisticPortfolios = new SynchronizedPessimisticPortfolios(settings.pessimisticPortfolios)

  private val dataUpdates = ConcurrentSubject.publish[WavesNodeEvent]

  // We need to store last two updates and consider them as stale, because we can face an issue during fullBalancesSnapshot
  //   when balance changes were deleted from LiquidBlock's diff, but haven't yet saved to DB.
  @volatile private var lastUpdates = List.empty[BlockchainBalance]
  private val maxPreviousBlockUpdates = 5 // + 1 = max size of lastUpdates. TODO why 5?

  override lazy val updates: Observable[WavesNodeUpdates] = Observable.fromFuture(meClient.currentBlockInfo)
    .flatMap { startBlockInfo =>
      log.info(s"Current block: $startBlockInfo")
      val startHeight = math.max(startBlockInfo.height - settings.maxRollbackHeight - 1, 1)
      val init: BlockchainStatus = BlockchainStatus.Normal(WavesChain(Vector.empty, startHeight, settings.maxRollbackHeight + 1))

      val combinedStream = new CombinedStream(settings.combinedStream, bClient.blockchainEvents, meClient.utxEvents)
      Observable(dataUpdates, combinedStream.stream)
        .merge
        .mapAccumulate(init) { case (origStatus, event) =>
          val x = StatusTransitions(origStatus, event)
          x.updatedLastBlockHeight match {
            case LastBlockHeight.Updated(to) => combinedStream.updateHeightHint(to)
            case LastBlockHeight.RestartRequired(from) => combinedStream.restartFrom(from)
            case _ =>
          }
          if (x.requestNextBlockchainEvent) bClient.blockchainEvents.requestNext()
          requestBalances(x.requestBalances)
          val updatedPessimistic = processUtxEvents(x.utxUpdate) // TODO DEX-1045 Do we need to filter out known transactions (WavesChain)?
          val changedAddresses = x.updatedBalances.regular.keySet ++ x.updatedBalances.outLeases.keySet ++ updatedPessimistic
          val updatedFinalBalances = changedAddresses
            .map { address =>
              address -> AddressBalanceUpdates(
                regular = x.updatedBalances.regular.getOrElse(address, Map.empty),
                outLease = x.updatedBalances.outLeases.get(address),
                pessimisticCorrection =
                  if (updatedPessimistic.contains(address)) pessimisticPortfolios.getAggregated(address)
                  else Map.empty
              )
            }
            .toMap

          if (!x.updatedBalances.isEmpty) {
            log.info(s"Updating.r: ${x.updatedBalances.regular}")
            lastUpdates = x.updatedBalances :: lastUpdates.take(maxPreviousBlockUpdates)
          } // TODO 2 blocks are enough?

          // // Not useful for UTX, because it doesn't consider the current state of orders
          // // Will be useful, when blockchain updates send order fills.
          // val fillsDebugInfo = x.utxUpdate.unconfirmedTxs.flatMap { tx =>
          //   val fills = tx.diff.toList.flatMap(_.orderFills).map { fill =>
          //     s"${fill.orderId.toVanilla} -> v:${fill.volume} + f:${fill.fee}"
          //   }
          //   if (fills.isEmpty) Nil
          //   else List(s"${tx.id.toVanilla}: ${fills.mkString(", ")}")
          // }
          //
          // if (fillsDebugInfo.nonEmpty) log.info(s"Detected fills:\n${fillsDebugInfo.mkString("\n")}")

          val unconfirmedTxs = for {
            tx <- x.utxUpdate.unconfirmedTxs.view if isExchangeTxFromMatcher(tx)
            signedTx <- tx.transaction
            changes <- tx.diff.flatMap(_.stateUpdate)
          } yield tx.id.toVanilla -> TransactionWithChanges(tx.id, signedTx, changes)

          val confirmedTxs = x.utxUpdate.confirmedTxs.view
            .collect { case (id, x) if isExchangeTxFromMatcher(x.tx) => id.toVanilla -> x }

          val failedTxs = for {
            tx <- x.utxUpdate.failedTxs.values.view if isExchangeTxFromMatcher(tx)
            signedTx <- tx.transaction
            changes <- tx.diff.flatMap(_.stateUpdate)
          } yield tx.id.toVanilla -> TransactionWithChanges(tx.id, signedTx, changes)

          (x.newStatus, WavesNodeUpdates(updatedFinalBalances, (unconfirmedTxs ++ confirmedTxs ++ failedTxs).toMap))
        }
        .filterNot(_.isEmpty)
        .tap(_ => combinedStream.startFrom(startHeight))
    }
    .doOnError(e => Task(log.error("Got an error in the combined stream", e)))

  // TODO DEX-1013
  private def processUtxEvents(utxUpdate: UtxUpdate): Set[Address] =
    if (utxUpdate.resetCaches) pessimisticPortfolios.replaceWith(utxUpdate.unconfirmedTxs)
    else
      pessimisticPortfolios.addPending(utxUpdate.unconfirmedTxs) |+|
      pessimisticPortfolios.processConfirmed(utxUpdate.confirmedTxs.keySet)._1 |+|
      pessimisticPortfolios.removeFailed(utxUpdate.failedTxs.keySet)

  // TODO DEX-1015
  private def requestBalances(x: DiffIndex): Unit =
    if (!x.isEmpty)
      meClient.getBalances(x).onComplete {
        case Success(r) => dataUpdates.onNext(WavesNodeEvent.DataReceived(r))
        case Failure(e) =>
          log.warn("Got an error during requesting balances", e)
          requestBalances(x)
      }

  private def isExchangeTxFromMatcher(tx: SignedTransaction): Boolean =
    tx.transaction.exists { tx =>
      tx.data.isExchange && ByteString.unsignedLexicographicalComparator().compare(tx.senderPublicKey, pbMatcherPublicKey) == 0
    }

  private def isExchangeTxFromMatcher(tx: UtxTransaction): Boolean = tx.transaction.exists(isExchangeTxFromMatcher)

  // TODO Seems not used
  override def partialBalancesSnapshot(address: Address, assets: Set[Asset]): Future[AddressBalanceUpdates] =
    (
      meClient.getAddressPartialRegularBalance(address, assets),
      if (assets.contains(Asset.Waves)) meClient.getOutLeasing(address).map(_.some) else Future.successful(none)
    ).mapN { case (regular, outLeasing) =>
      val response = BlockchainBalance(
        regular = Map(address -> regular),
        outLeases = outLeasing.fold(Map.empty[Address, Long])(x => Map(address -> x))
      )

      val prioritizedLastUpdates = lastUpdates :+ response
      val r = prioritizedLastUpdates.map(_.regular.getOrElse(address, Map.empty))
      log.info(s"prioritizedLastUpdates($address).r = $r")
      AddressBalanceUpdates(
        regular = foldRight(r).filter { case (asset, _) => assets.contains(asset) },
        outLease =
          if (outLeasing.isEmpty) none[Long]
          else prioritizedLastUpdates.map(_.outLeases.get(address)).foldLeft(none[Long])(_.orElse(_)),
        pessimisticCorrection = pessimisticPortfolios.getAggregated(address).filter {
          case (asset, _) => assets.contains(asset)
        }
      )
    }

  // TODO Optimize
  private def foldRight[K, V](xs: List[Map[K, V]]): Map[K, V] = xs.foldRight(Map.empty[K, V])(_ ++ _)

  override def fullBalancesSnapshot(address: Address, excludeAssets: Set[Asset]): Future[AddressBalanceUpdates] =
    (
      meClient.getAddressFullRegularBalance(address, excludeAssets),
      if (excludeAssets.contains(Asset.Waves)) Future.successful(none[Long]) else meClient.getOutLeasing(address).map(_.some)
    ).mapN { case (regular, outLeasing) =>
      val response = BlockchainBalance(
        regular = Map(address -> regular),
        outLeases = outLeasing.fold(Map.empty[Address, Long])(x => Map(address -> x))
      )

      val prioritizedLastUpdates = lastUpdates :+ response
      val r = prioritizedLastUpdates.map(_.regular.getOrElse(address, Map.empty))
      log.info(s"fullBalancesSnapshot($address).r = $r")
      AddressBalanceUpdates(
        regular = foldRight(r).filter { case (asset, _) => !excludeAssets.contains(asset) },
        outLease =
          if (outLeasing.isEmpty) none[Long] else prioritizedLastUpdates.map(_.outLeases.get(address)).foldLeft(none[Long])(_.orElse(_)),
        pessimisticCorrection = pessimisticPortfolios.getAggregated(address).filter {
          case (asset, _) => !excludeAssets.contains(asset)
        }
      )
    }

  // TODO DEX-1012
  override def isFeatureActivated(id: Short): Future[Boolean] =
    meClient.isFeatureActivated(id)

  // TODO DEX-353
  override def assetDescription(asset: IssuedAsset): Future[Option[BriefAssetDescription]] =
    meClient.assetDescription(asset)

  // TODO DEX-353
  override def hasScript(asset: IssuedAsset): Future[Boolean] = meClient.hasScript(asset)

  override def runScript(asset: IssuedAsset, input: ExchangeTransaction): Future[RunScriptResult] =
    meClient.runScript(asset, input)

  override def hasScript(address: Address): Future[Boolean] =
    meClient.hasScript(address)

  override def runScript(address: Address, input: Order): Future[RunScriptResult] =
    meClient.runScript(address, input)

  override def areKnown(txIds: Seq[ByteStr]): Future[Map[ByteStr, Boolean]] =
    meClient.areKnown(txIds)

  override def broadcastTx(tx: ExchangeTransaction): Future[BroadcastResult] = meClient.broadcastTx(tx)
  override def checkedBroadcastTx(tx: ExchangeTransaction): Future[CheckedBroadcastResult] = meClient.checkedBroadcastTx(tx)

  override def isOrderConfirmed(orderId: ByteStr): Future[Boolean] =
    meClient.isOrderConfirmed(orderId)

  override def close(): Future[Unit] =
    meClient.close().zip(bClient.close()).map(_ => ())

}

object CombinedWavesBlockchainClient {

  case class Settings(
    maxRollbackHeight: Int,
    combinedStream: CombinedStream.Settings,
    pessimisticPortfolios: SynchronizedPessimisticPortfolios.Settings
  )

}
