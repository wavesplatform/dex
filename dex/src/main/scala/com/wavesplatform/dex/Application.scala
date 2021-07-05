package com.wavesplatform.dex

import akka.Done
import akka.actor.typed.scaladsl.adapter._
import akka.actor.{typed, ActorRef, ActorSystem, CoordinatedShutdown, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.respondWithHeader
import akka.pattern.{ask, gracefulStop, CircuitBreaker}
import akka.stream.Materializer
import akka.util.Timeout
import cats.data.EitherT
import cats.instances.future.catsStdInstancesForFuture
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.semigroupal._
import cats.syntax.traverse._
import ch.qos.logback.classic.LoggerContext
import com.typesafe.config._
import com.wavesplatform.dex.actors.ActorSystemOps.ImplicitOps
import com.wavesplatform.dex.actors.address.{AddressActor, AddressDirectoryActor}
import com.wavesplatform.dex.actors.events.OrderEventsCoordinatorActor
import com.wavesplatform.dex.actors.orderbook.{AggregatedOrderBookActor, OrderBookActor, OrderBookSnapshotStoreActor}
import com.wavesplatform.dex.actors.tx.{ExchangeTransactionBroadcastActor, WriteExchangeTransactionActor}
import com.wavesplatform.dex.actors.{OrderBookAskAdapter, OrderBookDirectoryActor, RootActorSystem}
import com.wavesplatform.dex.api.http.headers.{CustomMediaTypes, MatcherHttpServer}
import com.wavesplatform.dex.api.http.routes.v1.OrderBookRoute
import com.wavesplatform.dex.api.http.routes.v0.{BalancesRoute, CancelRoute, DebugRoute, HistoryRoute, InfoRoute, MarketsRoute, PlaceRoute, RatesRoute, StatusRoute, TransactionsRoute}
import com.wavesplatform.dex.api.http.{CompositeHttpService, MetricHttpFlow, OrderBookHttpInfo}
import com.wavesplatform.dex.api.routes.ApiRoute
import com.wavesplatform.dex.api.ws.actors.{WsExternalClientDirectoryActor, WsInternalBroadcastActor}
import com.wavesplatform.dex.api.ws.routes.MatcherWebSocketRoute
import com.wavesplatform.dex.app._
import com.wavesplatform.dex.caches.{MatchingRulesCache, OrderFeeSettingsCache, RateCache}
import com.wavesplatform.dex.db._
import com.wavesplatform.dex.db.leveldb.{openDb, LevelDb}
import com.wavesplatform.dex.domain.account.{Address, AddressScheme, PublicKey}
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.domain.bytes.codec.Base58
import com.wavesplatform.dex.domain.utils.{EitherExt2, LoggerFacade, ScorexLogging}
import com.wavesplatform.dex.effect.{liftValueAsync, FutureResult}
import com.wavesplatform.dex.error.ErrorFormatterContext
import com.wavesplatform.dex.grpc.integration.clients.MatcherExtensionAssetsCachingClient
import com.wavesplatform.dex.grpc.integration.clients.combined.{AkkaCombinedStream, CombinedWavesBlockchainClient}
import com.wavesplatform.dex.grpc.integration.clients.domain.AddressBalanceUpdates
import com.wavesplatform.dex.grpc.integration.dto.BriefAssetDescription
import com.wavesplatform.dex.history.HistoryRouterActor
import com.wavesplatform.dex.logs.SystemInformationReporter
import com.wavesplatform.dex.model.{AssetPairBuilder, ExchangeTransactionCreator, Fee, OrderValidator, ValidationStages}
import com.wavesplatform.dex.queue.ValidatedCommandWithMeta.Offset
import com.wavesplatform.dex.queue._
import com.wavesplatform.dex.settings.MatcherSettings
import com.wavesplatform.dex.settings.utils.ConfigOps.ConfigOps
import com.wavesplatform.dex.time.NTP
import com.wavesplatform.dex.tool.{KamonTraceUtils, WaitOffsetTool}
import kamon.Kamon
import monix.execution.ExecutionModel
import mouse.any.anySyntaxMouse
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource

import java.io.File
import java.security.Security
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{blocking, Await, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

class Application(settings: MatcherSettings, config: Config)(implicit val actorSystem: ActorSystem) extends ScorexLogging {

  private val monixScheduler = monix.execution.Scheduler.Implicits.global.withExecutionModel(ExecutionModel.AlwaysAsyncExecution)
  private val grpcEc = actorSystem.dispatchers.lookup("akka.actor.grpc-dispatcher")
  private val levelDbEc = actorSystem.dispatchers.lookup("akka.actor.leveldb-dispatcher")

  private val cs = CoordinatedShutdown(actorSystem)

  implicit private val materializer = Materializer.matFromSystem(actorSystem)
  cs.addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "Materializer") { () =>
    materializer.shutdown()
    Future.successful(Done)
  }

  private val blacklistedAddresses = settings.blacklistedAddresses.map(Address.fromString(_).explicitGet())
  private val matchingRulesCache = new MatchingRulesCache(settings)
  private val orderFeeSettingsCache = new OrderFeeSettingsCache(settings.orderFee)

  private val maybeApiKeyHash: Option[Array[Byte]] = Option(settings.restApi.apiKeyHash) filter (_.nonEmpty) map Base58.decode
  private val processConsumedTimeout = new Timeout(settings.processConsumedTimeout * 2)

  private val matcherKeyPair = AccountStorage.load(settings.accountStorage).map(_.keyPair).explicitGet().unsafeTap { x =>
    log.info(s"The DEX's public key: ${Base58.encode(x.publicKey.arr)}, account address: ${x.publicKey.toAddress.stringRepr}")
  }

  private def matcherPublicKey: PublicKey = matcherKeyPair
  @volatile private var lastProcessedOffset = -1L

  @volatile private var status: MatcherStatus = MatcherStatus.Starting
  cs.addJvmShutdownHook(setStatus(MatcherStatus.Stopping))

  @volatile private var hasMatcherAccountScript = false
  private val orderBooks = new AtomicReference(Map.empty[AssetPair, Either[Unit, ActorRef]])
  private val snapshotsRestored = Promise[ValidatedCommandWithMeta.Offset]() // Earliest offset among snapshots

  private val time = new NTP(settings.ntpServer)
  cs.addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "NTP") { () =>
    Future { blocking(time.close()); Done }
  }

  private val db = openDb(settings.dataDirectory)
  private val asyncLevelDb = LevelDb.async(db)(levelDbEc)

  cs.addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "DB") { () =>
    Future { blocking(db.close()); Done }
  }

  private val assetPairsDb = AssetPairsDb.levelDb(asyncLevelDb)
  private val orderBookSnapshotDb = OrderBookSnapshotDb.levelDb(asyncLevelDb)
  private val orderDb = OrderDb.levelDb(settings.orderDb, asyncLevelDb)

  private val assetsCache = AssetsCache.from(AssetsDb.levelDb(asyncLevelDb))

  implicit private val errorContext: ErrorFormatterContext =
    ErrorFormatterContext.fromOptional(assetsCache.cached.get(_: Asset).map(_.decimals))

  private val matcherQueue: MatcherQueue = settings.eventsQueue.`type` match {
    case "local" =>
      log.info("Commands will be stored locally")
      new LocalMatcherQueue(settings.eventsQueue.local, LocalQueueStore.levelDb(asyncLevelDb), time)

    case "kafka" =>
      log.info("Commands will be stored in Kafka")
      new KafkaMatcherQueue(settings.eventsQueue.kafka)

    case x => throw new IllegalArgumentException(s"Unknown queue type: $x")
  }

  cs.addTask(CoordinatedShutdown.PhaseServiceStop, "Queue") { () =>
    matcherQueue.close(5.seconds).map(_ => Done)
  }

  private val orderBookAskAdapter = new OrderBookAskAdapter(orderBooks, settings.actorResponseTimeout)

  private val orderBookHttpInfo = new OrderBookHttpInfo(settings.orderBookHttp, orderBookAskAdapter, time, getAndCacheDecimals)

  private val transactionCreator = new ExchangeTransactionCreator(
    matcherKeyPair,
    settings.exchangeTxBaseFee,
    hasMatcherAccountScript,
    assetsCache.cached.unsafeGetHasScript // Should be in the cache, because assets decimals are required during an order book creation
  )

  private val wavesBlockchainAsyncClient = new MatcherExtensionAssetsCachingClient(
    underlying = CombinedWavesBlockchainClient(
      settings.wavesBlockchainClient,
      matcherPublicKey,
      monixScheduler = monixScheduler,
      grpcExecutionContext = grpcEc,
      mkCombinedStream = (meClient, buClient) =>
        new AkkaCombinedStream(
          settings.wavesBlockchainClient.combinedClientSettings.combinedStream,
          buClient.blockchainEvents,
          meClient.utxEvents
        )(actorSystem, monixScheduler)
    ),
    assetsCache = assetsCache
  )

  cs.addTask(CoordinatedShutdown.PhaseServiceStop, "Blockchain client") { () =>
    wavesBlockchainAsyncClient.close().map(_ => Done)
  }

  log.info("Loading known assets ...")
  private val allKnownAssetsFuture = loadAllKnownAssets()
  private val rateCacheFuture = RateCache(asyncLevelDb)

  private val cacheAndAssetFutures = for {
    _ <- allKnownAssetsFuture
    cache <- rateCacheFuture
  } yield cache

  private val rateCache = Await.result(cacheAndAssetFutures, 5.minutes)

  private val pairBuilder = new AssetPairBuilder(settings, getAndCacheDescription, settings.blacklistedAssets)

  private val txWriterRef =
    actorSystem.actorOf(WriteExchangeTransactionActor.props(ExchangeTxStorage.levelDB(asyncLevelDb)), WriteExchangeTransactionActor.name)

  private val wavesNetTxBroadcasterRef = actorSystem.spawn(
    ExchangeTransactionBroadcastActor(
      settings = ExchangeTransactionBroadcastActor.Settings(
        settings.exchangeTransactionBroadcast.interval,
        settings.exchangeTransactionBroadcast.maxPendingTime
      ),
      blockchain = wavesBlockchainAsyncClient.checkedBroadcastTx,
      time = time
    ),
    "exchange-transaction-broadcast"
  )

  private val wsInternalBroadcastRef: typed.ActorRef[WsInternalBroadcastActor.Command] = actorSystem.spawn(
    WsInternalBroadcastActor(settings.webSockets.internalBroadcast),
    "ws-internal-broadcast"
  )

  private val externalClientDirectoryRef: typed.ActorRef[WsExternalClientDirectoryActor.Message] =
    actorSystem.spawn(WsExternalClientDirectoryActor(), s"ws-external-cd-${ThreadLocalRandom.current().nextInt(Int.MaxValue)}")

  private val orderBookSnapshotStoreRef: ActorRef = actorSystem.actorOf(
    OrderBookSnapshotStoreActor.props(orderBookSnapshotDb),
    "order-book-snapshot-store"
  )

  private val historyRouterRef =
    if (settings.orderHistory.enable) actorSystem.actorOf(
      // Safe here, retrieve during OrderBookDirectoryActor starting, consuming or placing
      HistoryRouterActor.props(assetsCache.cached.unsafeGetDecimals, settings.postgres, settings.orderHistory),
      "history-router"
    ).some
    else none

  private val addressActorBlockchainInteraction = new AddressActor.BlockchainInteraction {

    override def getFullBalances(address: Address, exclude: Set[Asset]): Future[AddressBalanceUpdates] =
      wavesBlockchainAsyncClient.fullBalancesSnapshot(address, exclude)

  }

  private val addressDirectoryRef =
    actorSystem.actorOf(AddressDirectoryActor.props(orderDb, mkAddressActorProps, historyRouterRef), AddressDirectoryActor.name)

  private val storeBreaker = new CircuitBreaker(
    actorSystem.scheduler,
    maxFailures = settings.eventsQueue.circuitBreaker.maxFailures,
    callTimeout = settings.eventsQueue.circuitBreaker.callTimeout,
    resetTimeout = settings.eventsQueue.circuitBreaker.resetTimeout
  )

  private def storeCommand(payload: ValidatedCommand): Future[Option[ValidatedCommandWithMeta]] =
    storeBreaker.withCircuitBreaker(matcherQueue.store(payload))

  private def mkAddressActorProps(address: Address, recovered: Boolean): Props = AddressActor.props(
    address,
    time,
    orderDb,
    ValidationStages.mkSecond(wavesBlockchainAsyncClient, orderBookAskAdapter),
    storeCommand,
    recovered,
    addressActorBlockchainInteraction,
    settings.addressActor,
    asset => assetsCache.cached.unsafeGet(asset)
  )

  private val orderEventsCoordinatorRef = actorSystem.spawn(
    behavior = OrderEventsCoordinatorActor.apply(
      settings = settings.orderEventsCoordinatorActor,
      addressDirectoryRef = addressDirectoryRef,
      dbWriterRef = txWriterRef,
      broadcasterRef = wavesNetTxBroadcasterRef,
      createTransaction = transactionCreator.createTransaction
    ),
    name = "events-coordinator"
  )

  private val orderBookDirectoryActorRef: ActorRef = {
    def mkOrderBookProps(assetPair: AssetPair, orderBookDirectoryActor: ActorRef): Props = {
      // Safe here, because we receive this information during OrderBookDirectoryActor starting, placing or consuming
      val amountAssetDecimals = errorContext.unsafeAssetDecimals(assetPair.amountAsset)
      val priceAssetDecimals = errorContext.unsafeAssetDecimals(assetPair.priceAsset)
      matchingRulesCache.setCurrentMatchingRuleForNewOrderBook(assetPair, lastProcessedOffset, amountAssetDecimals, priceAssetDecimals)
      OrderBookActor.props(
        OrderBookActor.Settings(AggregatedOrderBookActor.Settings(settings.webSockets.externalClientHandler.messagesInterval)),
        orderBookDirectoryActor,
        orderEventsCoordinatorRef,
        orderBookSnapshotStoreRef,
        wsInternalBroadcastRef,
        assetPair,
        time,
        matchingRules = matchingRulesCache.getMatchingRules(assetPair, amountAssetDecimals, priceAssetDecimals),
        updateCurrentMatchingRules = actualMatchingRule => matchingRulesCache.updateCurrentMatchingRule(assetPair, actualMatchingRule),
        normalizeMatchingRule = denormalizedMatchingRule => denormalizedMatchingRule.normalize(amountAssetDecimals, priceAssetDecimals),
        getMakerTakerFeeByOffset = Fee.getMakerTakerFeeByOffset(orderFeeSettingsCache),
        restrictions = settings.orderRestrictions.get(assetPair) // TODO Move this and webSocketSettings to OrderBook's settings
      )
    }

    actorSystem.actorOf(
      OrderBookDirectoryActor.props(
        settings,
        assetPairsDb,
        {
          case Right(startOffset) => snapshotsRestored.success(startOffset)
          case Left(msg) => snapshotsRestored.failure(RecoveryError(msg))
        },
        orderBooks,
        mkOrderBookProps,
        assetsCache,
        pairBuilder.quickValidateAssetPair
      ),
      OrderBookDirectoryActor.name
    )
  }

  cs.addTask(CoordinatedShutdown.PhaseServiceRequestsDone, "Actors") { () =>
    gracefulStop(orderBookDirectoryActorRef, 3.seconds, OrderBookDirectoryActor.Shutdown).map(_ => Done)
  }

  private val placeRoute: PlaceRoute = {
    val orderValidation = ValidationStages.mkFirst(
      settings,
      matcherPublicKey,
      orderFeeSettingsCache,
      matchingRulesCache,
      rateCache,
      getAndCacheDescription,
      wavesBlockchainAsyncClient,
      transactionCreator,
      time,
      orderBookAskAdapter,
      lastProcessedOffset,
      blacklistedAddresses,
      hasMatcherAccountScript
    )(_)
    new PlaceRoute(pairBuilder, addressDirectoryRef, orderValidation, settings, () => status, maybeApiKeyHash)
  }

  private val cancelRoute = new CancelRoute(pairBuilder, addressDirectoryRef, () => status, orderDb, maybeApiKeyHash, settings)
  private val ratesRoute = new RatesRoute(pairBuilder, () => status, maybeApiKeyHash, rateCache, externalClientDirectoryRef)
  private val historyRoute = new HistoryRoute(pairBuilder, addressDirectoryRef, () => status, maybeApiKeyHash, settings)
  private val statusRoute = new StatusRoute(pairBuilder, addressDirectoryRef, () => status, orderDb, maybeApiKeyHash, settings)
  private val balancesRoute = new BalancesRoute(pairBuilder, addressDirectoryRef, () => status, maybeApiKeyHash, settings)
  private val transactionsRoute = new TransactionsRoute(() => status, orderDb, maybeApiKeyHash)

  private val debugRoute = new DebugRoute(
    config,
    orderBookDirectoryActorRef,
    addressDirectoryRef,
    wavesBlockchainAsyncClient.status(),
    () => status,
    () => lastProcessedOffset,
    () => matcherQueue.lastOffset,
    maybeApiKeyHash,
    settings
  )

  private val marketsRoute = new MarketsRoute(
    pairBuilder,
    matcherPublicKey,
    orderBookDirectoryActorRef,
    matcherQueue.store,
    p => Option(orderBooks.get()) flatMap (_ get p),
    orderBookHttpInfo,
    getActualTickSize = { assetPair =>
      getAndCacheDecimals(assetPair.amountAsset)
        .product(getAndCacheDecimals(assetPair.priceAsset))
        .map { case (amountAssetDecimals, priceAssetDecimals) =>
          matchingRulesCache.getDenormalizedRuleForNextOrder(assetPair, lastProcessedOffset, amountAssetDecimals, priceAssetDecimals).tickSize
        }
    },
    settings,
    () => status,
    maybeApiKeyHash
  )

  private val infoRoute = new InfoRoute(
    matcherPublicKey,
    settings,
    () => status,
    ExchangeTransactionCreator.getAdditionalFeeForScript(hasMatcherAccountScript),
    maybeApiKeyHash,
    rateCache,
    validatedAllowedOrderVersions = () => {
      Future
        .sequence {
          settings.allowedOrderVersions.map(OrderValidator.checkOrderVersion(_, wavesBlockchainAsyncClient.isFeatureActivated).value)
        }
        .map(_.collect { case Right(version) => version })
    },
    () => orderFeeSettingsCache.getSettingsForOffset(lastProcessedOffset + 1)
  )

  private val v0HttpRoute =
    Seq(placeRoute, cancelRoute, debugRoute, infoRoute, ratesRoute, historyRoute, statusRoute, transactionsRoute, balancesRoute, marketsRoute)

  private val v1HttpRoute = Seq(OrderBookRoute(pairBuilder, orderBookHttpInfo, () => status, maybeApiKeyHash))

  private val wsApiRoute = new MatcherWebSocketRoute(
    wsInternalBroadcastRef,
    externalClientDirectoryRef,
    addressDirectoryRef,
    orderBookDirectoryActorRef,
    time,
    pairBuilder,
    maybeApiKeyHash,
    settings,
    () => status,
    () => rateCache.getAllRates
  )

  cs.addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "WebSockets")(() => wsApiRoute.gracefulShutdown().map(_ => Done))

  private val matcherApiRoutes: Seq[ApiRoute] = v0HttpRoute ++ v1HttpRoute

  private val matcherApiTypes: Set[Class[_]] = matcherApiRoutes.map(_.getClass).toSet

  private val startGuard = for {
    (_, http) <- {
      log.info("Checking matcher's account script ...")
      wavesBlockchainAsyncClient.hasScript(matcherKeyPair).map(hasMatcherAccountScript = _)
    } zip {
      Future(
        blocking {
          log.info(s"Initializing HTTP ...")
          Http() // May take 3+ seconds
        }
      )
    }

    _ = {
      log.info("Start watching Node updates")
      // Note, updates is lazy, so it is initialized here
      wavesBlockchainAsyncClient.updates
        .dropWhile { case (_, isReady) => !isReady }
        .foreach { update =>
          orderEventsCoordinatorRef ! OrderEventsCoordinatorActor.Command.ApplyNodeUpdates(update._1)
        }(monixScheduler)
    }

    _ <- {
      log.info(s"Preparing HTTP service (Matcher's ID = ${settings.id}) ...")
      // Indirectly initializes OrderBookDirectoryActor, so it must be after loadAllKnownAssets
      val combinedRoute = respondWithHeader(MatcherHttpServer(settings.id)) {
        new CompositeHttpService(matcherApiTypes, matcherApiRoutes, settings.restApi).compositeRoute
      }

      log.info(s"Binding REST and WebSocket API ${settings.restApi.address}:${settings.restApi.port} ...")
      http
        .newServerAt(settings.restApi.address, settings.restApi.port)
        .adaptSettings { settings =>
          settings.withParserSettings(settings.parserSettings.withCustomMediaTypes(CustomMediaTypes.`application/hocon`))
        }
        .bindFlow(MetricHttpFlow.metricFlow(combinedRoute))
        .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 5.seconds))
    }.map { serverBinding =>
      log.info(s"REST and WebSocket API bound to ${serverBinding.localAddress}")
    }

    earliestSnapshotOffset <- {
      log.info("Waiting all snapshots are restored ...")
      waitSnapshotsRestored(settings.snapshotsLoadingTimeout).andThen(_ => log.info("All snapshots are restored"))
    }

    deadline = settings.startEventsProcessingTimeout.fromNow
    (firstQueueOffset, lastOffsetQueue) <- {
      log.info("Getting queue offsets ...")
      val requests = new RepeatableRequests(matcherQueue, deadline)
      requests.firstOffset zip requests.lastOffset
    }

    firstConsumedOffset = earliestSnapshotOffset + 1

    _ <- {
      log.info(s"Offsets: earliest snapshot = $earliestSnapshotOffset, first = $firstQueueOffset, last = $lastOffsetQueue")
      if (lastOffsetQueue < earliestSnapshotOffset)
        Future.failed(RecoveryError("The queue doesn't have messages to recover all orders and continue work. Did you change the queue?"))
      else {
        // if lastOffsetQueue == earliestSnapshotOffset
        // then no commands will be processed and lastProcessedOffset will not be changed from -1
        if (lastOffsetQueue == earliestSnapshotOffset)
          lastProcessedOffset = lastOffsetQueue
        if (earliestSnapshotOffset < firstQueueOffset) {
          /*
           If Kafka has the wrong retention config, there are only two possible outcomes:
           1. Throw an error - then the state of the Matcher should be reset, otherwise, we couldn't start it.
              So we lost all orders including order books
           2. Ignore and warn - then we could have probably unexpected matches, but orders will be preserved and we don't need to reset
              the state of the Matcher in order to start it. We chose the second.
           */
          log.warn(s"The queue doesn't contain required offsets to recover all orders. Check retention settings of the queue. Continue...")
          Future.unit
        } else Future.unit
      }
    }
    _ <- Future {
      log.info("Starting consuming")
      matcherQueue.startConsume(firstConsumedOffset, consumeMessages)
    } zip {
      log.info(s"Last queue offset is $lastOffsetQueue")
      WaitOffsetTool.waitOffsetReached(
        getLastQueueOffset(deadline),
        lastProcessedOffset,
        lastOffsetQueue,
        deadline,
        settings.waitingOffsetTool,
        actorSystem.scheduler
      )
    }
  } yield {
    log.info("Last offset has been reached, switching to a normal mode")
    addressDirectoryRef ! AddressDirectoryActor.Command.StartWork
  }

  startGuard.onComplete {
    case Success(_) => setStatus(MatcherStatus.Working)
    case Failure(e: ApplicationStopReason) => forceStopApplication(e)
    case Failure(e) =>
      log.error(s"Can't start matcher: ${e.getMessage}", e)
      forceStopApplication(StartingMatcherError)
  }

  private def loadAllKnownAssets(): Future[Unit] =
    assetPairsDb.all().map(_.flatMap(_.assets) ++ settings.mentionedAssets).flatMap { assetsToLoad =>
      Future
        .traverse(assetsToLoad)(asset => getAndCacheDecimals(asset).value tupleLeft asset)
        .map { xs =>
          val notFoundAssets = xs.collect { case (id, Left(_)) => id }
          if (notFoundAssets.nonEmpty) {
            log.error(s"Can't load assets: ${notFoundAssets.mkString(", ")}")
            forceStopApplication(NotSynchronizedNodeError)
          }
        }
    }

  // DEX-1192 docs/places-and-cancels.md
  private def consumeMessages(xs: List[ValidatedCommandWithMeta]): Future[Unit] =
    if (xs.isEmpty) Future.unit
    else {
      val eventAssets = xs.flatMap(_.command.assets)
      val loadAssets = eventAssets.traverse(getAndCacheDescription).value

      loadAssets.flatMap { _ =>
        val assetPairs: Set[AssetPair] = xs
          .map { validatedCommandWithMeta =>
            lazy val handleValidatedCommandWithMeta = {
              log.debug(s"Consumed $validatedCommandWithMeta")
              orderBookDirectoryActorRef ! validatedCommandWithMeta
              lastProcessedOffset = validatedCommandWithMeta.offset
              validatedCommandWithMeta.command.assetPair
            }

            validatedCommandWithMeta.command.maybeCtx.fold(handleValidatedCommandWithMeta) { ctx =>
              if (status == MatcherStatus.Working) {
                val parentSpan = ctx.get(kamon.trace.Span.Key)
                val span =
                  Kamon.spanBuilder(s"consumedValidatedCommandWithMeta")
                    .asChildOf(parentSpan)
                    .traceId(parentSpan.trace.id)
                    .tag(ctx.tags)
                    .samplingDecision(KamonTraceUtils.Sample)
                    .doNotTrackMetrics()
                    .start()
                Kamon.runWithSpan[AssetPair](span)(handleValidatedCommandWithMeta)
              } else
                handleValidatedCommandWithMeta
            }
          }
          .to(Set)

        orderBookDirectoryActorRef
          .ask(OrderBookDirectoryActor.PingAll(assetPairs))(processConsumedTimeout)
          .recover { case NonFatal(e) => log.error("PingAll is timed out!", e) }
          .map(_ => ())
      } andThen {
        case Failure(ex) =>
          log.error("Error while event processing occurred: ", ex)
          forceStopApplication(EventProcessingError)
        case _ =>
      }
    }

  private def setStatus(newStatus: MatcherStatus): Unit = {
    status = newStatus
    log.info(s"Status now is $newStatus")
  }

  private def waitSnapshotsRestored(wait: FiniteDuration): Future[ValidatedCommandWithMeta.Offset] = Future.firstCompletedOf(
    List(
      snapshotsRestored.future,
      actorSystem.timeout(wait).recover { case _ => throw RecoveryError(s"Timeout of $wait for waiting snapshots to restore is out") }
    )
  )

  private def getLastQueueOffset(deadline: Deadline): Future[Offset] =
    new RepeatableRequests(matcherQueue, deadline).lastOffset

  private def getAndCacheDecimals(asset: Asset): FutureResult[Int] = getAndCacheDescription(asset).map(_.decimals)

  private def getAndCacheDescription(asset: Asset): FutureResult[BriefAssetDescription] =
    asset match {
      case Asset.Waves => liftValueAsync(BriefAssetDescription.wavesDescription)
      case asset: Asset.IssuedAsset =>
        EitherT(wavesBlockchainAsyncClient.assetDescription(asset).map {
          case Some(x) => x.asRight
          case None => error.AssetNotFound(asset).asLeft
        })
    }

}

object Application {

  def main(args: Array[String]): Unit = {

    // prevents java from caching successful name resolutions, which is needed e.g. for proper NTP server rotation
    // http://stackoverflow.com/a/17219327
    System.setProperty("sun.net.inetaddr.ttl", "0")
    System.setProperty("sun.net.inetaddr.negative.ttl", "0")
    Security.setProperty("networkaddress.cache.ttl", "0")
    Security.setProperty("networkaddress.cache.negative.ttl", "0")

    // specify aspectj to use it's build-in infrastructure
    // http://www.eclipse.org/aspectj/doc/released/pdguide/trace.html
    System.setProperty("org.aspectj.tracing.factory", "default")

    val configFile = args.headOption
    val (config, settings) = loadApplicationConfig(configFile.map(new File(_)))

    val excludedConfigKeys = settings.secureKeys
    val filterredConfig = config.withoutKeys(excludedConfigKeys)

    // This option is used in logback.xml by default
    if (Option(System.getProperty("waves.dex.root-directory")).isEmpty)
      System.setProperty("waves.dex.root-directory", config.getString("waves.dex.root-directory"))

    val log = LoggerFacade(LoggerFactory getLogger getClass)

    if (config.getBoolean("kamon.enable")) {
      // IMPORTANT: to make use of default settings for histograms and timers, it's crucial to reconfigure Kamon with
      //            our merged config BEFORE initializing any metrics, including in settings-related companion objects
      Kamon.init(config)
      log.info("Enabled kamon metrics")
    }

    log.info("Starting...")

    RootActorSystem.start("wavesplatform", config) { implicit actorSystem =>
      log.info(s"${s"DEX v${Version.VersionString}"} Blockchain Id: ${settings.addressSchemeCharacter}")

      val cs = CoordinatedShutdown(actorSystem)

      cs.addCancellableJvmShutdownHook {
        SystemInformationReporter.report(filterredConfig)
      }

      cs.addCancellableTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "Kamon") { () =>
        Kamon.stopModules().map(_ => Done)
      }

      cs.addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "Logger") { () =>
        val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
        Future { blocking(loggerContext.stop()); Done }
      }

      new Application(settings, filterredConfig)
    }
  }

  private def loadApplicationConfig(external: Option[File]): (Config, MatcherSettings) = {

    import com.wavesplatform.dex.settings.loadConfig
    import com.wavesplatform.dex.settings.utils.ConfigOps.ConfigOps

    val config = loadConfig(external map ConfigFactory.parseFile)
    val scalaContextPath = "scala.concurrent.context"

    config.getConfig(scalaContextPath).toProperties.asScala.foreach { case (k, v) => System.setProperty(s"$scalaContextPath.$k", v) }

    val settings = ConfigSource.fromConfig(config).at("waves.dex").loadOrThrow[MatcherSettings]

    // Initialize global var with actual address scheme
    AddressScheme.current = new AddressScheme { override val chainId: Byte = settings.addressSchemeCharacter.toByte }

    (config, settings)
  }

}
