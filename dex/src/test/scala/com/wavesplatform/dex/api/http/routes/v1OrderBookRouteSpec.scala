package com.wavesplatform.dex.api.http.routes

import akka.actor.ActorRef
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.testkit.{TestActor, TestProbe}
import cats.syntax.either._
import cats.syntax.option._
import com.typesafe.config.ConfigFactory
import com.wavesplatform.dex._
import com.wavesplatform.dex.actors.OrderBookAskAdapter
import com.wavesplatform.dex.actors.orderbook.AggregatedOrderBookActor
import com.wavesplatform.dex.api.RouteSpec
import com.wavesplatform.dex.api.http.ApiMarshallers._
import com.wavesplatform.dex.api.http.entities.{HttpOrderBook, HttpV1LevelAgg, HttpV1OrderBook}
import com.wavesplatform.dex.api.http.routes.v1.OrderBookRoute
import com.wavesplatform.dex.api.http.{entities, OrderBookHttpInfo}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.db.WithDb
import com.wavesplatform.dex.domain.asset.Asset.Waves
import com.wavesplatform.dex.domain.crypto
import com.wavesplatform.dex.effect.{liftErrorAsync, liftFutureAsync, liftValueAsync}
import com.wavesplatform.dex.error.ErrorFormatterContext
import com.wavesplatform.dex.grpc.integration.dto.BriefAssetDescription
import com.wavesplatform.dex.model.{AssetPairBuilder, LevelAgg, OrderBookAggregatedSnapshot}
import com.wavesplatform.dex.settings.MatcherSettings
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.Eventually
import pureconfig.ConfigSource

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class v1OrderBookRouteSpec extends RouteSpec("/api/v1") with MatcherSpecBase with PathMockFactory with Eventually with WithDb {

  private val settings =
    ConfigSource.fromConfig(ConfigFactory.load()).at("waves.dex").loadOrThrow[MatcherSettings].copy(priceAssets = Seq(usd, Waves))

  implicit private val efc: ErrorFormatterContext = {
    case `usd` => 2.some
    case _ => 8.some
  }

  private val wavesUsdAggregatedSnapshot = OrderBookAggregatedSnapshot(
    bids = Seq(
      LevelAgg(43800.waves, 118),
      LevelAgg(52187.waves, 117),
      LevelAgg(809.waves, 116)
    ),
    asks = Seq(
      LevelAgg(2134.waves, 119),
      LevelAgg(747.waves, 120)
    )
  )

  private def test[U](f: Route => U, apiKey: String = ""): U = {

    val orderBookActor = TestProbe("orderBook")

    orderBookActor.setAutoPilot { (sender: ActorRef, msg: Any) =>
      msg match {
        case request: AggregatedOrderBookActor.Query.GetHttpView =>
          val entity =
            HttpOrderBook(
              0L,
              wavesUsdPair,
              wavesUsdAggregatedSnapshot.bids,
              wavesUsdAggregatedSnapshot.asks,
              Some(8 -> 2)
            )

          val httpResponse =
            HttpResponse(
              entity = HttpEntity(
                ContentTypes.`application/json`,
                HttpOrderBook.toJson(entity)
              )
            )

          request.client ! httpResponse

        case _ =>
      }

      TestActor.KeepRunning
    }

    val orderBooks = new AtomicReference(Map(wavesUsdPair -> orderBookActor.ref.asRight[Unit]))
    val orderBookAskAdapter = new OrderBookAskAdapter(orderBooks, 5.seconds)

    val orderBookHttpInfo =
      new OrderBookHttpInfo(
        settings = settings.orderBookHttp,
        askAdapter = orderBookAskAdapter,
        time = time,
        assetDecimals = {
          case a if a == `usd` || a == Waves =>
            efc.assetDecimals(a) match {
              case Some(decimals) => liftValueAsync(decimals)
              case None => liftErrorAsync(error.UnexpectedError)
            }
          case x => liftFutureAsync(Future.failed(new IllegalArgumentException(s"No information about $x")))
        }
      )

    val route =
      OrderBookRoute(
        assetPairBuilder = new AssetPairBuilder(
          settings,
          {
            case `usd` => liftValueAsync(BriefAssetDescription("USD", 8, hasScript = false, isNft = false))
            case x => liftErrorAsync[BriefAssetDescription](error.AssetNotFound(x))
          },
          Set.empty
        ),
        orderBookHttpInfo = orderBookHttpInfo,
        matcherStatus = () => MatcherStatus.Working,
        apiKeyHash = Some(crypto secureHash apiKey)
      )

    f(route.route)
  }

  // getOrderBook
  routePath("/orderbook/{amountAsset}/{priceAsset}") - {
    "returns an order book" in test { route =>
      Get(routePath(s"/orderbook/WAVES/${usd.id}")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpV1OrderBook] should matchTo(
          entities.HttpV1OrderBook(
            timestamp = 0L,
            bids = wavesUsdAggregatedSnapshot.bids.toList.map(HttpV1LevelAgg.fromLevelAgg(_, wavesUsdPair)),
            asks = wavesUsdAggregatedSnapshot.asks.toList.map(HttpV1LevelAgg.fromLevelAgg(_, wavesUsdPair))
          )
        )
      }
    }
  }
}
