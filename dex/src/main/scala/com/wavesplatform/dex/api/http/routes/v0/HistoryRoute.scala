package com.wavesplatform.dex.api.http.routes.v0

import akka.actor.ActorRef
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server._
import akka.stream.Materializer
import akka.util.Timeout
import com.wavesplatform.dex._
import com.wavesplatform.dex.actors.address.AddressActor
import com.wavesplatform.dex.actors.address.AddressActor.OrderListType
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives._
import com.wavesplatform.dex.api.http.directives.ProtectDirective
import com.wavesplatform.dex.api.http.entities._
import com.wavesplatform.dex.api.http.protocol.HttpCancelOrder
import com.wavesplatform.dex.api.http.{HasStatusBarrier, _}
import com.wavesplatform.dex.api.routes.PathMatchers.{AddressPM, AssetPairPM, PublicKeyPM}
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.domain.account.Address
import com.wavesplatform.dex.domain.asset.AssetPair
import com.wavesplatform.dex.domain.order.Order.Id
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.model.{AssetPairBuilder, OrderInfo, OrderStatus}
import com.wavesplatform.dex.settings.MatcherSettings
import io.swagger.annotations._

import javax.ws.rs.Path
import scala.concurrent.ExecutionContext

@Path("/matcher")
@Api()
class HistoryRoute(
  assetPairBuilder: AssetPairBuilder,
  addressActor: ActorRef,
  override val matcherStatus: () => MatcherStatus,
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

  override lazy val route: Route = pathPrefix("matcher") {
    pathPrefix("orderbook") {
      matcherStatusBarrier {
        getOrderHistoryByAssetPairAndPKWithSig ~ getOrderHistoryByPKWithSig ~ deleteOrderFromHistoryById
      }
    } ~ pathPrefix("orders") {
      matcherStatusBarrier(getOrderHistoryByAddressWithKey)
    }
  }

  private val tupledOrderBookHistoryItem: ((Id, OrderInfo[OrderStatus])) => HttpOrderBookHistoryItem =
    Function.tupled(HttpOrderBookHistoryItem.fromOrderInfo)

  private def loadOrders(address: Address, pair: Option[AssetPair], orderListType: OrderListType): Route = complete {
    askMapAddressActor[AddressActor.Reply.GetOrderStatuses](addressActor, address, AddressActor.Query.GetOrdersStatuses(pair, orderListType)) {
      reply =>
        reply.xs.map(tupledOrderBookHistoryItem)
    }
  }

  private def getOrderListType(activeOnly: Option[Boolean], closedOnly: Option[Boolean], default: OrderListType): OrderListType =
    if (activeOnly.isEmpty && closedOnly.isEmpty) default
    else
      (activeOnly.getOrElse(false), closedOnly.getOrElse(false)) match {
        case (true, true) => OrderListType.Empty
        case (false, true) => OrderListType.ClosedOnly
        case (true, false) => OrderListType.ActiveOnly
        case (false, false) => OrderListType.All
      }

  @Path("/orderbook/{amountAsset}/{priceAsset}/delete#deleteOrderFromHistoryById")
  @Deprecated
  @ApiOperation(
    value = "Delete Order from History by Id",
    notes = "This method is deprecated and doesn't work anymore. Please don't use it.",
    httpMethod = "POST",
    produces = "application/json",
    consumes = "application/json",
    tags = Array("history"),
    response = classOf[HttpSuccessfulDelete]
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
  def deleteOrderFromHistoryById: Route =
    path(AssetPairPM / "delete") { _ =>
      (withMetricsAndTraces("deleteOrderFromHistoryById") & protect) {
        post {
          entity(as[HttpCancelOrder]) { req =>
            complete {
              req.orderId.fold[MatcherResponse](NotImplemented(error.FeatureNotImplemented))(OrderDeleted)
            }
          }
        } ~ get(complete(StatusCodes.MethodNotAllowed))
      }
    }

  @Path("/orderbook/{amountAsset}/{priceAsset}/publicKey/{publicKey}#getOrderHistoryByAssetPairAndPKWithSig")
  @ApiOperation(
    value = "Order History by Asset Pair and Public Key",
    notes = "Get Order History for a given Asset Pair and Public Key",
    httpMethod = "GET",
    tags = Array("history"),
    response = classOf[Array[HttpOrderBookHistoryItem]]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "amountAsset", value = "Amount Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "priceAsset", value = "Price Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "publicKey", value = "Public Key", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(
        name = "activeOnly",
        value = "Return active only orders (Accepted and PartiallyFilled)",
        required = false,
        dataType = "boolean",
        paramType = "query",
        defaultValue = "false"
      ),
      new ApiImplicitParam(
        name = "closedOnly",
        value = "Return closed only orders (Filled and Canceled)",
        required = false,
        dataType = "boolean",
        paramType = "query",
        defaultValue = "false"
      ),
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
  def getOrderHistoryByAssetPairAndPKWithSig: Route =
    (path(AssetPairPM / "publicKey" / PublicKeyPM) & get) { (pairOrError, publicKeyOrError) =>
      (withMetricsAndTraces("getOrderHistoryByAssetPairAndPKWithSig") & protect) {
        withPublicKey(publicKeyOrError) { publicKey =>
          withAssetPair(assetPairBuilder, pairOrError, redirectToInverse = true, s"/publicKey/$publicKey") { pair =>
            parameters("activeOnly".as[Boolean].?, "closedOnly".as[Boolean].?) { (activeOnly, closedOnly) =>
              signedGet(publicKey) {
                loadOrders(publicKey, Some(pair), getOrderListType(activeOnly, closedOnly, OrderListType.All))
              }
            }
          }
        }
      }
    }

  @Path("/orderbook/{publicKey}#getOrderHistoryByPKWithSig")
  @ApiOperation(
    value = "Order History by Public Key",
    notes = "Get Order History for a given Public Key",
    httpMethod = "GET",
    tags = Array("history"),
    response = classOf[Array[HttpOrderBookHistoryItem]]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "publicKey", value = "Public Key", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(
        name = "activeOnly",
        value = "Return active only orders (Accepted and PartiallyFilled)",
        required = false,
        dataType = "boolean",
        paramType = "query",
        defaultValue = "false"
      ),
      new ApiImplicitParam(
        name = "closedOnly",
        value = "Return closed only orders (Filled and Canceled)",
        required = false,
        dataType = "boolean",
        paramType = "query",
        defaultValue = "false"
      ),
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
  def getOrderHistoryByPKWithSig: Route =
    (path(PublicKeyPM) & get) { publicKeyOrError =>
      (withMetricsAndTraces("getOrderHistoryByPKWithSig") & protect) {
        parameters("activeOnly".as[Boolean].?, "closedOnly".as[Boolean].?) { (activeOnly, closedOnly) =>
          withPublicKey(publicKeyOrError) { publicKey =>
            signedGet(publicKey) {
              loadOrders(publicKey, None, getOrderListType(activeOnly, closedOnly, OrderListType.All))
            }
          }
        }
      }
    }

  @Path("/orders/{address}#getOrderHistoryByAddressWithKey")
  @ApiOperation(
    value = "All Order History by Address. Requires API Key",
    notes = "Get All Order History for a given address",
    httpMethod = "GET",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("history"),
    response = classOf[Array[HttpOrderBookHistoryItem]]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "address", value = "Address", dataType = "string", paramType = "path"),
      new ApiImplicitParam(
        name = "activeOnly",
        value = "Return active only orders (Accepted and PartiallyFilled)",
        required = false,
        dataType = "boolean",
        paramType = "query",
        defaultValue = "false"
      ),
      new ApiImplicitParam(
        name = "closedOnly",
        value = "Return closed only orders (Filled and Canceled)",
        required = false,
        dataType = "boolean",
        paramType = "query",
        defaultValue = "false"
      )
    )
  )
  def getOrderHistoryByAddressWithKey: Route =
    (path(AddressPM) & get) { addressOrError =>
      (withMetricsAndTraces("getOrderHistoryByApiKey") & protect) {
        (withAuth & withUserPublicKeyOpt) { userPublicKey =>
          withAddress(addressOrError) { address =>
            userPublicKey match {
              case Some(upk) if upk.toAddress != address => invalidUserPublicKey
              case _ =>
                parameters("activeOnly".as[Boolean].?, "closedOnly".as[Boolean].?) { (activeOnly, closedOnly) =>
                  loadOrders(address, None, getOrderListType(activeOnly, closedOnly, OrderListType.ActiveOnly))
                }
            }
          }
        }
      }
    }

}
