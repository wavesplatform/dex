package com.wavesplatform.dex.api.http.routes.v0

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{PathMatcher, Route}
import akka.stream.Materializer
import akka.util.Timeout
import cats.syntax.option._
import com.wavesplatform.dex.actors.address.AddressActor
import com.wavesplatform.dex.api.http.HasStatusBarrier
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives.withMetricsAndTraces
import com.wavesplatform.dex.api.http.directives.ProtectDirective
import com.wavesplatform.dex.api.http.entities.{HttpError, HttpSuccessfulPlace, SimpleResponse}
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.domain.order.Order
import com.wavesplatform.dex.domain.order.OrderJson.orderFormat
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.effect.FutureResult
import com.wavesplatform.dex.error
import com.wavesplatform.dex.metrics.TimerExt
import com.wavesplatform.dex.model.AssetPairBuilder
import com.wavesplatform.dex.settings.MatcherSettings
import io.swagger.annotations.{Api, _}

import javax.ws.rs.Path
import scala.concurrent.{ExecutionContext, Future}

@Path("/matcher")
@Api()
class PlaceRoute(
  assetPairBuilder: AssetPairBuilder,
  addressActor: ActorRef,
  orderValidator: Order => FutureResult[Order],
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

  override lazy val route: Route = pathPrefix("matcher" / "orderbook")(placeLimitOrder ~ placeMarketOrder)

  // DEX-1192 docs/places-and-cancels.md
  private def placeOrder(endpoint: Option[PathMatcher[Unit]], isMarket: Boolean): Route = {
    val orderType = if (isMarket) "Market" else "Limit"
    val route = (pathEndOrSingleSlash & post & withMetricsAndTraces(s"place${orderType}Order") & protect & entity(as[Order])) { order =>
      withAssetPair(assetPairBuilder, Right(order.assetPair), formatError = e => StatusCodes.BadRequest -> HttpError.from(e, "OrderRejected")) {
        _ =>
          complete(
            placeTimer.measureFuture {
              orderValidator(order).value flatMap {
                case Right(o) =>
                  placeTimer.measureFuture {
                    askAddressActor(addressActor, o.sender, AddressActor.Command.PlaceOrder(o, isMarket)) {
                      case AddressActor.Event.OrderAccepted(x) => SimpleResponse(HttpSuccessfulPlace(x))
                      case x: error.MatcherError =>
                        if (x == error.CanNotPersistEvent) StatusCodes.ServiceUnavailable -> HttpError.from(x, "WavesNodeUnavailable")
                        else StatusCodes.BadRequest -> HttpError.from(x, "OrderRejected")
                    }
                  }
                case Left(e) => Future.successful[ToResponseMarshallable](StatusCodes.BadRequest -> HttpError.from(e, "OrderRejected"))
              }
            }
          )
      }
    }

    endpoint.fold(route)(path(_)(route))
  }

  @Path("/orderbook#placeLimitOrder")
  @ApiOperation(
    value = "Place Limit Order",
    notes = "Place a new limit order (buy or sell)",
    httpMethod = "POST",
    produces = "application/json",
    consumes = "application/json",
    tags = Array("place"),
    response = classOf[HttpSuccessfulPlace]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(
        name = "body",
        value = "Json with data",
        required = true,
        paramType = "body",
        dataType = "com.wavesplatform.dex.domain.order.OrderV3"
      )
    )
  )
  def placeLimitOrder: Route = placeOrder(none, isMarket = false)

  @Path("/orderbook/market#placeMarketOrder")
  @ApiOperation(
    value = "Place Market Order",
    notes = "Place a new market order (buy or sell)",
    httpMethod = "POST",
    produces = "application/json",
    consumes = "application/json",
    tags = Array("place"),
    response = classOf[HttpSuccessfulPlace]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(
        name = "body",
        value = "Json with data",
        required = true,
        paramType = "body",
        dataType = "com.wavesplatform.dex.domain.order.OrderV3"
      )
    )
  )
  def placeMarketOrder: Route = placeOrder(PathMatcher("market").some, isMarket = true)

}
