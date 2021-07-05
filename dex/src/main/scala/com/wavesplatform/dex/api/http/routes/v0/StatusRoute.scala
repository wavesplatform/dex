package com.wavesplatform.dex.api.http.routes.v0

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.ToResponseMarshallable
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
import com.wavesplatform.dex.api.routes.PathMatchers.{AddressPM, AssetPairPM, OrderPM, PublicKeyPM}
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.db.OrderDb
import com.wavesplatform.dex.domain.account.Address
import com.wavesplatform.dex.domain.order.Order
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.model.{AssetPairBuilder, _}
import com.wavesplatform.dex.settings.MatcherSettings
import io.swagger.annotations._

import javax.ws.rs.Path
import scala.concurrent.{ExecutionContext, Future}

@Path("/matcher")
@Api()
class StatusRoute(
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
        matcherStatusBarrier {
          getOrderStatusByPKAndIdWithSig ~ getOrderStatusByAssetPairAndId
        }
      } ~ pathPrefix("orders") {
        matcherStatusBarrier(getOrderStatusByAddressAndIdWithKey)
      }
    }

  private def getOrderStatusInfo(id: Order.Id, address: Address): StandardRoute = complete {
    askMapAddressActor[AddressActor.Reply.GetOrdersStatusInfo](addressActor, address, AddressActor.Query.GetOrderStatusInfo(id)) {
      _.maybeOrderStatusInfo match {
        case Some(oi) => SimpleResponse(HttpOrderBookHistoryItem.fromOrderInfo(id, oi))
        case None => InfoNotFound(error.OrderNotFound(id))
      }
    }
  }

  @Path("/orders/{address}/{orderId}#getOrderStatusByAddressAndIdWithKey")
  @ApiOperation(
    value = "Order Status Info by Address and ID without signature. Requires API Key",
    notes = "Get Status Info of the specified order for a given address without signature",
    httpMethod = "GET",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("status"),
    response = classOf[HttpOrderBookHistoryItem]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "address", value = "Address", dataType = "string", paramType = "path"),
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
  def getOrderStatusByAddressAndIdWithKey: Route =
    (path(AddressPM / OrderPM) & get) { (addressOrError, orderIdOrError) =>
      (withMetricsAndTraces("getOrderStatusByAddressAndIdWithKey") & protect) {
        (withAuth & withUserPublicKeyOpt) {
          userPublicKey =>
            withAddress(addressOrError) { address =>
              withOrderId(orderIdOrError) { orderId =>
                userPublicKey match {
                  case Some(upk) if upk.toAddress != address => invalidUserPublicKey
                  case _ => getOrderStatusInfo(orderId, address)
                }
              }
            }
        }
      }
    }

  // https://github.com/OAI/OpenAPI-Specification/issues/146#issuecomment-117288707
  @Path("/orderbook/{publicKey}/{orderId}#getOrderStatusByPKAndIdWithSig")
  @ApiOperation(
    value = "Order Status Info by Public Key and ID",
    notes = "Get Status Info of the specified order for a given public key",
    httpMethod = "GET",
    tags = Array("status"),
    response = classOf[HttpOrderBookHistoryItem]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "publicKey", value = "Public Key", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "orderId", value = "Order ID", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "Timestamp", value = "Timestamp", required = true, dataType = "integer", paramType = "header"),
      new ApiImplicitParam(
        name = "Signature",
        value = "Base58 encoded Curve25519.sign(senderPrivateKey, concat(bytesOf(publicKey), bigEndianBytes(Timestamp)))",
        required = true,
        dataType = "string",
        paramType = "header"
      )
    )
  )
  def getOrderStatusByPKAndIdWithSig: Route =
    (path(PublicKeyPM / OrderPM) & get) { (publicKeyOrError, orderIdOrError) =>
      (withMetricsAndTraces("getOrderStatusByPKAndIdWithSig") & protect) {
        withOrderId(orderIdOrError) { orderId =>
          withPublicKey(publicKeyOrError) { publicKey =>
            signedGet(publicKey) {
              getOrderStatusInfo(orderId, publicKey.toAddress)
            }
          }
        }
      }
    }

  @Path("/orderbook/{amountAsset}/{priceAsset}/{orderId}#getOrderStatusByAssetPairAndId")
  @ApiOperation(
    value = "Order Status",
    notes = "Get Order status for a given Asset Pair during the last 30 days",
    httpMethod = "GET",
    tags = Array("status"),
    response = classOf[HttpOrderStatus]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "amountAsset", value = "Amount Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "priceAsset", value = "Price Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "orderId", value = "Order ID", required = true, dataType = "string", paramType = "path")
    )
  )
  def getOrderStatusByAssetPairAndId: Route = (path(AssetPairPM / OrderPM) & get) { (pairOrError, orderIdOrError) =>
    (withMetricsAndTraces("getOrderStatusByAssetPairAndId") & protect) {
      withOrderId(orderIdOrError) { orderId =>
        withAssetPair(assetPairBuilder, pairOrError, redirectToInverse = true, s"/$orderId") { _ =>
          val future =
            for {
              maybeOrder <- orderDb.get(orderId)
              result <- {
                maybeOrder match {
                  case Some(order) =>
                    askMapAddressActor[AddressActor.Reply.GetOrderStatus](addressActor, order.sender, AddressActor.Query.GetOrderStatus(orderId)) {
                      r =>
                        HttpOrderStatus.from(r.x)
                    }
                  case None =>
                    orderDb
                      .getOrderInfo(orderId)
                      .map(_.fold(HttpOrderStatus.from(OrderStatus.NotFound))(x => HttpOrderStatus.from(x.status)))
                      .map(ToResponseMarshallable(_))
                }
              }
            } yield result

          complete {
            future.recover { case th =>
              log.error("error while retrieving order status", th)
              ToResponseMarshallable(entities.InternalError)
            }
          }
        }
      }
    }
  }

}
