package com.wavesplatform.dex.api.http.routes.v1

import akka.http.scaladsl.server.Route
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives.withMetricsAndTraces
import com.wavesplatform.dex.api.http.entities.HttpV1OrderBook
import com.wavesplatform.dex.api.http.{HasStatusBarrier, OrderBookHttpInfo}
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute, PathMatchers}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.error.ErrorFormatterContext
import com.wavesplatform.dex.model.{AssetPairBuilder, MatcherModel}
import io.swagger.annotations.{Api, ApiImplicitParam, ApiImplicitParams, ApiOperation}

import javax.ws.rs.Path
import scala.concurrent.ExecutionContext

@Path("/api/v1")
@Api(value = "/api v1/")
case class OrderBookRoute(
  assetPairBuilder: AssetPairBuilder,
  orderBookHttpInfo: OrderBookHttpInfo,
  matcherStatus: () => MatcherStatus,
  apiKeyHash: Option[Array[Byte]]
)(implicit val errorContext: ErrorFormatterContext, ex: ExecutionContext)
    extends ApiRoute
    with AuthRoute
    with HasStatusBarrier
    with ScorexLogging {

  import PathMatchers._

  override lazy val route: Route = pathPrefix("api" / "v1") {
    matcherStatusBarrier(getOrderBook)
  }

  @Path("/orderbook/{amountAsset}/{priceAsset}#V1getOrderBook")
  @ApiOperation(
    value = "Get Order Book for a given Asset Pair",
    notes = "Get Order Book for a given Asset Pair",
    httpMethod = "GET",
    response = classOf[HttpV1OrderBook]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "amountAsset", value = "Amount Asset Id in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "priceAsset", value = "Price Asset Id in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
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
    (path("orderbook" / AssetPairPM) & get) { pairOrError =>
      withMetricsAndTraces("V1.getOrderBook") {
        withAssetPair(assetPairBuilder, pairOrError) { p =>
          parameters(Symbol("depth").as[Int].?) {
            depth =>
              withAssetPair(assetPairBuilder, Right(p), redirectToInverse = true) {
                pair =>
                  complete(orderBookHttpInfo.getHttpView(pair, MatcherModel.Denormalized, depth))
              }
          }
        }
      }
    }

}
