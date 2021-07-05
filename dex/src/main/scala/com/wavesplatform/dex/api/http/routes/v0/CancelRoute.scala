package com.wavesplatform.dex.api.http.routes.v0

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server._
import akka.stream.Materializer
import akka.util.Timeout
import com.wavesplatform.dex._
import com.wavesplatform.dex.actors.address.AddressActor
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives._
import com.wavesplatform.dex.api.http.directives.ProtectDirective
import com.wavesplatform.dex.api.http.entities._
import com.wavesplatform.dex.api.http.headers.`X-User-Public-Key`
import com.wavesplatform.dex.api.http.{HasStatusBarrier, _}
import com.wavesplatform.dex.api.routes.PathMatchers.{AddressPM, AssetPairPM, OrderPM}
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.db.OrderDb
import com.wavesplatform.dex.domain.account.Address
import com.wavesplatform.dex.domain.asset.AssetPair
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.model.AssetPairBuilder
import com.wavesplatform.dex.settings.MatcherSettings
import io.swagger.annotations._

import javax.ws.rs.Path
import scala.concurrent.{ExecutionContext, Future}

@Path("/matcher")
@Api()
class CancelRoute(
  assetPairBuilder: AssetPairBuilder,
  addressActor: ActorRef,
  override val matcherStatus: () => MatcherStatus,
  orderDb: OrderDb[Future],
  override val apiKeyHash: Option[Array[Byte]],
  matcherSettings: MatcherSettings
)(implicit mat: Materializer)
    extends ApiRoute
    with ProtectDirective
    with HasStatusBarrier
    with AuthRoute
    with ScorexLogging {

  implicit private val executionContext: ExecutionContext = mat.executionContext
  implicit private val timeout: Timeout = matcherSettings.actorResponseTimeout

  override lazy val route: Route =
    pathPrefix("matcher") {
      pathPrefix("orderbook") {
        matcherStatusBarrier(cancelOneOrAllInPairOrdersWithSig ~ cancelAllOrdersWithSig)
      } ~ pathPrefix("orders") {
        matcherStatusBarrier(cancelOrdersByIdsWithKey ~ cancelOneOrderWithKey)
      }
    }

  private def handleCancelRequestToRoute(assetPair: Option[AssetPair], sender: Address, orderId: Option[ByteStr], timestamp: Option[Long]): Route =
    complete(handleCancelRequestToFuture(assetPair, sender, orderId, timestamp): Future[ToResponseMarshallable])

  private def handleCancelRequestToRoute(assetPair: Option[AssetPair]): Route =
    withCancelRequest { req =>
      handleCancelRequestToRoute(assetPair, req.sender, req.orderId, req.timestamp)
    }

  private val handleBatchCancelResponse: LogicResponseHandler = {
    case AddressActor.Event.BatchCancelCompleted(xs) =>
      HttpSuccessfulBatchCancel(
        xs.map {
          case (id, Right(_)) => Right(HttpSuccessfulSingleCancel(id))
          case (_, Left(e)) => Left(HttpError.from(e, "OrderCancelRejected"))
        }.toList
      )
    case x: error.MatcherError => StatusCodes.ServiceUnavailable -> HttpError.from(x, "BatchCancelRejected")
  }

  private def handleCancelRequestToFuture(
    assetPair: Option[AssetPair],
    sender: Address,
    orderId: Option[ByteStr],
    timestamp: Option[Long]
  ): Future[ToResponseMarshallable] =
    (timestamp, orderId) match {
      case (Some(ts), None) =>
        askAddressActor(addressActor, sender, AddressActor.Command.CancelAllOrders(assetPair, ts, AddressActor.Command.Source.Request))(
          handleBatchCancelResponse
        )
      case (None, Some(oid)) =>
        askAddressActor(addressActor, sender, AddressActor.Command.CancelOrder(oid, AddressActor.Command.Source.Request)) {
          case AddressActor.Event.OrderCanceled(x) => SimpleResponse(HttpSuccessfulSingleCancel(x))
          case x: error.MatcherError =>
            if (x == error.CanNotPersistEvent) StatusCodes.ServiceUnavailable -> HttpError.from(x, "WavesNodeUnavailable")
            else StatusCodes.BadRequest -> HttpError.from(x, "OrderCancelRejected")
        }
      case _ =>
        Future.successful(StatusCodes.BadRequest -> HttpError.from(error.CancelRequestIsIncomplete, "OrderCancelRejected"))
    }

  @Path("/orderbook/{amountAsset}/{priceAsset}/cancel#cancelOneOrAllInPairOrdersWithSig")
  @ApiOperation(
    value = "Cancel Order",
    notes = "Cancel previously submitted Order if it's not already filled completely",
    httpMethod = "POST",
    produces = "application/json",
    consumes = "application/json",
    tags = Array("cancel"),
    response = classOf[HttpSuccessfulCancel]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "amountAsset", value = "Amount Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "priceAsset", value = "Price Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(
        name = "body",
        value = "Json with data",
        required = true,
        paramType = "body",
        dataType = "com.wavesplatform.dex.api.http.protocol.HttpCancelOrder"
      )
    )
  )
  def cancelOneOrAllInPairOrdersWithSig: Route = // DEX-1192 docs/places-and-cancels.md
    (path(AssetPairPM / "cancel") & post) { pairOrError =>
      (withMetricsAndTraces("cancelOneOrAllInPairOrdersWithSig") & protect) {
        withAssetPair(assetPairBuilder, pairOrError, formatError = e => OrderCancelRejected(e)) { pair =>
          handleCancelRequestToRoute(Some(pair))

        }
      }
    }

  @Path("/orderbook/cancel#cancelAllOrdersWithSig")
  @ApiOperation(
    value = "Cancel all active orders",
    httpMethod = "POST",
    produces = "application/json",
    consumes = "application/json",
    tags = Array("cancel"),
    response = classOf[HttpSuccessfulBatchCancel]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(
        name = "body",
        value = "Json with data",
        required = true,
        paramType = "body",
        dataType = "com.wavesplatform.dex.api.http.protocol.HttpCancelOrder"
      )
    )
  )
  def cancelAllOrdersWithSig: Route =
    (path("cancel") & post)((withMetricsAndTraces("cancelAllOrdersWithSig") & protect)(handleCancelRequestToRoute(None)))

  @Path("/orders/{address}/cancel#cancelOrdersByIdsWithKey")
  @ApiOperation(
    value = "Cancel active orders by IDs. Requires API Key",
    httpMethod = "POST",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    produces = "application/json",
    consumes = "application/json",
    tags = Array("cancel"),
    response = classOf[HttpSuccessfulBatchCancel]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "address", value = "Address", dataType = "string", paramType = "path"),
      new ApiImplicitParam(
        name = "body",
        value = "Json array with Order IDs",
        required = true,
        paramType = "body",
        dataTypeClass = classOf[Array[String]]
      )
    )
  )
  def cancelOrdersByIdsWithKey: Route =
    (path(AddressPM / "cancel") & post) { addressOrError =>
      (withMetricsAndTraces("cancelAllByApiKeyAndIds") & protect) {
        (withAuth & withUserPublicKeyOpt) { userPublicKey =>
          withAddress(addressOrError) { address =>
            userPublicKey match {
              case Some(upk) if upk.toAddress != address => invalidUserPublicKey
              case _ =>
                entity(as[Set[ByteStr]]) { xs =>
                  complete {
                    askAddressActor(addressActor, address, AddressActor.Command.CancelOrders(xs, AddressActor.Command.Source.Request))(
                      handleBatchCancelResponse
                    )
                  }
                }
            }
          }
        }
      }
    }

  @Path("/orders/cancel/{orderId}#cancelOneOrderWithKey")
  @ApiOperation(
    value = "Cancel Order by ID without signature. Requires API Key",
    notes = "Cancel Order by ID without signature",
    httpMethod = "POST",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("cancel"),
    response = classOf[HttpSuccessfulSingleCancel]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "orderId", value = "Order ID", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(
        name = `X-User-Public-Key`.headerName,
        value = "User's public key",
        required = false,
        dataType = "string",
        paramType = "header",
        defaultValue = ""
      )
    )
  )
  def cancelOneOrderWithKey: Route =
    (path("cancel" / OrderPM) & post) { orderIdOrError =>
      (withMetricsAndTraces("cancelOneOrderWithKey") & protect) {
        (withAuth & withUserPublicKeyOpt) { maybeUserPublicKey =>
          withOrderId(orderIdOrError) { orderId =>
            complete {
              maybeUserPublicKey match {
                case Some(userPublicKey) =>
                  handleCancelRequestToFuture(None, userPublicKey.toAddress, Some(orderId), None)
                case None =>
                  orderDb.get(orderId).flatMap {
                    case Some(order) => handleCancelRequestToFuture(None, order.sender, Some(orderId), None)
                    case None => Future.successful(ToResponseMarshallable(OrderCancelRejected(error.OrderNotFound(orderId))))
                  }.recover { case th =>
                    log.error("error while cancelling order", th)
                    ToResponseMarshallable(entities.InternalError)
                  }
              }
            }
          }
        }
      }
    }

}
