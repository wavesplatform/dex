package com.wavesplatform.dex.api.http.routes.v0

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server._
import akka.pattern.ask
import akka.stream.Materializer
import akka.util.Timeout
import cats.instances.future._
import cats.instances.list._
import cats.syntax.traverse._
import com.wavesplatform.dex._
import com.wavesplatform.dex.actors.OrderBookDirectoryActor._
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives._
import com.wavesplatform.dex.api.http.directives.ProtectDirective
import com.wavesplatform.dex.api.http.entities._
import com.wavesplatform.dex.api.http.{HasStatusBarrier, OrderBookHttpInfo, _}
import com.wavesplatform.dex.api.routes.PathMatchers.AssetPairPM
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.domain.account.PublicKey
import com.wavesplatform.dex.domain.asset.AssetPair
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.effect.FutureResult
import com.wavesplatform.dex.model.{AssetPairBuilder, _}
import com.wavesplatform.dex.queue.MatcherQueue.StoreValidatedCommand
import com.wavesplatform.dex.queue.ValidatedCommand
import com.wavesplatform.dex.settings.MatcherSettings
import io.swagger.annotations._

import javax.ws.rs.Path
import scala.concurrent.ExecutionContext

@Path("/matcher")
@Api()
class MarketsRoute(
  assetPairBuilder: AssetPairBuilder,
  matcherPublicKey: PublicKey,
  matcher: ActorRef,
  storeCommand: StoreValidatedCommand,
  orderBook: AssetPair => Option[Either[Unit, ActorRef]],
  orderBookHttpInfo: OrderBookHttpInfo,
  getActualTickSize: AssetPair => FutureResult[BigDecimal],
  matcherSettings: MatcherSettings,
  override val matcherStatus: () => MatcherStatus,
  override val apiKeyHash: Option[Array[Byte]]
)(implicit mat: Materializer)
    extends ApiRoute
    with ProtectDirective
    with HasStatusBarrier
    with AuthRoute
    with ScorexLogging {

  implicit private val executionContext: ExecutionContext = mat.executionContext
  implicit private val timeout: Timeout = matcherSettings.actorResponseTimeout

  override lazy val route: Route =
    pathPrefix("matcher" / "orderbook") {
      matcherStatusBarrier {
        getOrderBookRestrictions ~ getOrderBook ~ getOrderBookStatus ~ getOrderBooks ~ deleteOrderBookWithKey
      }
    }

  @Path("/orderbook/{amountAsset}/{priceAsset}#getOrderBook")
  @ApiOperation(
    value = "Get Order Book for a given Asset Pair",
    notes = "Get Order Book for a given Asset Pair",
    httpMethod = "GET",
    tags = Array("markets"),
    response = classOf[HttpV0OrderBook]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "amountAsset", value = "Amount Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "priceAsset", value = "Price Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(
        name = "depth",
        value = "Limit the number of bid/ask records returned",
        required = false,
        dataType = "integer",
        paramType = "query"
      )
    )
  )
  def getOrderBook: Route =
    (path(AssetPairPM) & get) { pairOrError =>
      (withMetricsAndTraces("getOrderBook") & protect) {
        parameters("depth".as[String].?) {
          case None => withAssetPair(assetPairBuilder, pairOrError, redirectToInverse = true, "") { pair =>
              complete(orderBookHttpInfo.getHttpView(pair, MatcherModel.Normalized, None))
            }
          case Some(depth) =>
            depth.toIntOption match {
              case None => complete(InvalidDepth(s"Depth value '$depth' must be an Integer"))
              case Some(d) =>
                if (d >= 0) withAssetPair(assetPairBuilder, pairOrError, redirectToInverse = true, s"?depth=$d") { pair =>
                  complete(orderBookHttpInfo.getHttpView(pair, MatcherModel.Normalized, Some(d)))
                }
                else complete(InvalidDepth(s"Depth value '$depth' must be non-negative"))
            }
        }
      }
    }

  @Path("/orderbook/{amountAsset}/{priceAsset}/status#getOrderBookStatus")
  @ApiOperation(
    value = "Get Market Status",
    notes = "Get current market data such as last trade, best bid and ask",
    httpMethod = "GET",
    tags = Array("markets"),
    response = classOf[HttpOrderBookStatus]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "amountAsset", value = "Amount Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "priceAsset", value = "Price Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path")
    )
  )
  def getOrderBookStatus: Route =
    (path(AssetPairPM / "status") & get) { pairOrError =>
      (withMetricsAndTraces("getOrderBookStatus") & protect) {
        withAssetPair(assetPairBuilder, pairOrError, redirectToInverse = true, suffix = "/status") { pair =>
          complete(orderBookHttpInfo.getMarketStatus(pair))
        }
      }
    }

  @Path("/orderbook/{amountAsset}/{priceAsset}/info#getOrderBookRestrictions")
  @ApiOperation(
    value = "Get Order Restrictions for the specified Asset Pair",
    httpMethod = "GET",
    tags = Array("markets"),
    response = classOf[HttpOrderBookInfo]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "amountAsset", value = "Amount Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "priceAsset", value = "Price Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path")
    )
  )
  def getOrderBookRestrictions: Route =
    (path(AssetPairPM / "info") & get) { pairOrError =>
      (withMetricsAndTraces("getOrderBookRestrictions") & protect) {
        withAssetPair(assetPairBuilder, pairOrError, redirectToInverse = true, suffix = "/info") { pair =>
          complete(getOrderBookRestrictions(pair).value.map {
            case Right(x) => SimpleResponse(x)
            case Left(e) => InfoNotFound(e)
          })
        }
      }
    }

  private def getOrderBookRestrictions(pair: AssetPair): FutureResult[HttpOrderBookInfo] = getActualTickSize(pair).map { tickSize =>
    HttpOrderBookInfo(
      restrictions = matcherSettings.orderRestrictions.get(pair).map(HttpOrderRestrictions.fromSettings),
      matchingRules = HttpMatchingRules(tickSize = tickSize.toDouble)
    )
  }

  @Path("/orderbook#getOrderBooks")
  @ApiOperation(
    value = "Get the open trading markets",
    notes = "Get the open trading markets along with trading pairs meta data",
    httpMethod = "GET",
    tags = Array("markets"),
    response = classOf[HttpTradingMarkets]
  )
  def getOrderBooks: Route =
    (pathEndOrSingleSlash & get) {
      (withMetricsAndTraces("getOrderBooks") & protect) {
        complete(
          (matcher ? GetMarkets).mapTo[List[MarketData]].flatMap { markets =>
            markets
              .map { md =>
                getOrderBookRestrictions(md.pair)
                  .map { meta =>
                    HttpMarketDataWithMeta(
                      md.pair.amountAsset,
                      md.amountAssetName,
                      md.amountAssetInfo.map(HttpAssetInfo.fromAssetInfo),
                      md.pair.priceAsset,
                      md.priceAssetName,
                      md.priceAssetInfo.map(HttpAssetInfo.fromAssetInfo),
                      md.created,
                      meta.restrictions,
                      meta.matchingRules
                    )
                  }
                  .value
              }
              .sequence
              .map(_.collect { case Right(x) => x })
              .map(x => SimpleResponse(HttpTradingMarkets(matcherPublicKey, x)))
          }
        )
      }
    }

  @Path("/orderbook/{amountAsset}/{priceAsset}#deleteOrderBookWithKey")
  @ApiOperation(
    value = "Remove Order Book for a given Asset Pair. Requires API Key",
    notes = "Remove Order Book for a given Asset Pair. Attention! Use this method only when clients can't place orders on this pair!",
    httpMethod = "DELETE",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("markets"),
    response = classOf[HttpMessage]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "amountAsset", value = "Amount Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "priceAsset", value = "Price Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path")
    )
  )
  def deleteOrderBookWithKey: Route =
    (path(AssetPairPM) & delete) { pairOrError =>
      (withMetricsAndTraces("deleteOrderBookWithKey") & protect & withAuth) {
        withAssetPair(assetPairBuilder, pairOrError, validate = false) { pair =>
          orderBook(pair) match {
            case Some(Right(_)) =>
              complete(
                storeCommand(ValidatedCommand.DeleteOrderBook(pair))
                  .map {
                    case None => NotImplemented(error.FeatureDisabled)
                    case _ => SimpleResponse(StatusCodes.Accepted, "Deleting order book")
                  }
                  .recover { case e: Throwable =>
                    log.error("Can not persist event", e)
                    CanNotPersist(error.CanNotPersistEvent)
                  }
              )
            case _ => complete(OrderBookUnavailable(error.OrderBookBroken(pair)))
          }
        }
      }
    }

}
