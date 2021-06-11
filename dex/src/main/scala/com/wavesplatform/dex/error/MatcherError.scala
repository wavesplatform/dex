package com.wavesplatform.dex.error

import akka.http.scaladsl.model.{HttpHeader, HttpResponse, StatusCode}
import com.wavesplatform.dex.api.ws.headers.{`X-Error-Code`, `X-Error-Message`}
import com.wavesplatform.dex.domain.account.{Address, PublicKey}
import com.wavesplatform.dex.domain.asset.Asset.{IssuedAsset, Waves}
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.domain.feature.BlockchainFeature
import com.wavesplatform.dex.domain.model.Denormalization
import com.wavesplatform.dex.domain.order.{Order, OrderType}
import com.wavesplatform.dex.error.Class.{common => commonClass, _}
import com.wavesplatform.dex.error.Entity.{common => commonEntity, _}
import com.wavesplatform.dex.error.Implicits._
import com.wavesplatform.dex.settings.{DeviationsSettings, OrderRestrictionsSettings}
import play.api.libs.json.{JsObject, Json, OWrites}

sealed abstract class MatcherError(val code: Int, val message: MatcherErrorMessage) extends Product with Serializable {
  def this(obj: Entity, part: Entity, klass: Class, message: MatcherErrorMessage) = this(
    (obj.code << 20) + (part.code << 8) + klass.code, // 32 bits = (12 for object) + (12 for part) + (8 for class)
    message
  )

  override def toString: String = s"${getClass.getCanonicalName}(error=$code,message=${message.text})"
}

object MatcherError {

  implicit val matcherErrorWrites: OWrites[MatcherError] = OWrites { x =>
    val obj = x.message
    val wrappedParams = if (obj.params == JsObject.empty) obj.params else Json.obj("params" -> obj.params)
    Json
      .obj(
        "error" -> x.code,
        "message" -> obj.text,
        "template" -> obj.template
      )
      .deepMerge(wrappedParams)
  }

  implicit final class Ops(val self: MatcherError) extends AnyVal {

    def toWsHttpResponse(statusCode: StatusCode): HttpResponse =
      HttpResponse(
        status = statusCode,
        headers = List[HttpHeader](`X-Error-Message`(self.message.text), `X-Error-Code`(self.code.toString))
      )

  }

}

case class Amount(asset: Asset, volume: BigDecimal)

object Amount {

  private[error] def apply(asset: Asset, volume: Long)(implicit efc: ErrorFormatterContext): Amount =
    new Amount(asset, Denormalization.denormalizeAmountAndFee(volume, efc.unsafeAssetDecimals(asset)))

}

case class Price(assetPair: AssetPair, volume: BigDecimal)

object Price {

  private[error] def apply(assetPair: AssetPair, volume: Long)(implicit efc: ErrorFormatterContext): Price =
    new Price(
      assetPair,
      Denormalization.denormalizePrice(volume, efc.unsafeAssetDecimals(assetPair.amountAsset), efc.unsafeAssetDecimals(assetPair.priceAsset))
    )

}

case class MatcherErrorMessage(text: String, template: String, params: JsObject)

case object MatcherIsStarting extends MatcherError(commonEntity, commonEntity, starting, e"System is starting. Please retry later")
case object MatcherIsStopping extends MatcherError(commonEntity, commonEntity, stopping, e"System is shutting down. Please retry later")
case object RequestTimeout extends MatcherError(request, commonEntity, stopping, e"Request timed out. Please retry later")
case object FeatureNotImplemented extends MatcherError(commonEntity, feature, unsupported, e"This feature is not implemented")
case object FeatureDisabled extends MatcherError(commonEntity, feature, disabled, e"This feature is disabled, contact with the administrator")
case object Balancing extends MatcherError(webSocket, commonEntity, optimization, e"System is balancing the load. Please reconnect")

case class OrderBookNotFound(theAssetPair: AssetPair)
  extends MatcherError(
    orderBook,
    commonEntity,
    notFound,
    e"The order book for ${Symbol("assetPair") -> theAssetPair} has been deleted or does not exist"
  )

case class OrderBookBroken(theAssetPair: AssetPair)
    extends MatcherError(
      orderBook,
      commonEntity,
      broken,
      e"The order book for ${Symbol("assetPair") -> theAssetPair} is unavailable, please contact with the administrator"
    )

case class OrderBookUnexpectedState(assetPair: AssetPair)
    extends MatcherError(
      orderBook,
      commonEntity,
      unexpected,
      e"The order book for ${Symbol("assetPair") -> assetPair} is unexpected state, please contact with the administrator"
    )

case class OrderBookStopped(assetPair: AssetPair)
    extends MatcherError(
      orderBook,
      commonEntity,
      disabled,
      e"The order book for ${Symbol("assetPair") -> assetPair} is stopped, please contact with the administrator"
    )

case object CanNotPersistEvent
    extends MatcherError(commonEntity, producer, broken, e"Can not persist command, please retry later or contact with the administrator")

case object CancelRequestIsIncomplete extends MatcherError(request, commonEntity, unexpected, e"Either timestamp or orderId must be specified")

case class UnexpectedMatcherPublicKey(required: PublicKey, given: PublicKey)
    extends MatcherError(
      commonEntity,
      pubKey,
      unexpected,
      e"The required matcher public key for this DEX is ${Symbol("required") -> required}, but given ${Symbol("given") -> given}"
    )

case class OrderInvalidSignature(id: Order.Id, details: String)
    extends MatcherError(
      order,
      signature,
      commonClass,
      e"The signature of order ${Symbol("id") -> id} is invalid: ${Symbol("details") -> details}"
    )

case class UnexpectedFeeAsset(required: Set[Asset], given: Asset)
    extends MatcherError(
      order,
      fee,
      unexpected,
      e"Required one of the following fee asset: ${Symbol("required") -> required}. But given ${Symbol("given") -> given}"
    )

case class FeeNotEnough(required: Amount, given: Amount)
    extends MatcherError(
      order,
      fee,
      notEnough,
      e"Required ${Symbol("required") -> required} as fee for this order, but given ${Symbol("given") -> given}"
    )

object FeeNotEnough {

  def apply(required: Long, given: Long, asset: Asset)(implicit efc: ErrorFormatterContext): FeeNotEnough = {
    val decimals = efc.unsafeAssetDecimals(asset)
    new FeeNotEnough(
      required = Amount(asset, Denormalization.denormalizeAmountAndFee(required, decimals)),
      given = Amount(asset, Denormalization.denormalizeAmountAndFee(given, decimals))
    )
  }

}

case class AssetNotFound(theAsset: IssuedAsset)
    extends MatcherError(asset, commonEntity, notFound, e"The asset ${Symbol("assetId") -> theAsset} not found")

case class CanNotCreateExchangeTransaction(details: String)
    extends MatcherError(exchangeTx, order, commonClass, e"Can't verify the order by an exchange transaction: ${Symbol("details") -> details}")

case class WrongExpiration(currentTs: Long, minExpirationOffset: Long, givenExpiration: Long)
    extends MatcherError(
      order,
      expiration,
      notEnough,
      e"""The expiration should be at least
                     |${Symbol("currentTimestamp") -> currentTs} + ${Symbol("minExpirationOffset") -> minExpirationOffset} = ${Symbol(
        "minExpiration"
      ) -> (currentTs + minExpirationOffset)},
                     |but it is ${Symbol("given") -> givenExpiration}"""
    )

case class OrderCommonValidationFailed(details: String)
    extends MatcherError(order, commonEntity, commonClass, e"The order is invalid: ${Symbol("details") -> details}")

case class InvalidAsset(theAsset: String, reason: String = "It should be 'WAVES' or a Base58 string")
    extends MatcherError(
      asset,
      commonEntity,
      broken,
      e"The asset '${Symbol("assetId") -> theAsset}' is wrong, reason: ${Symbol("reason") -> reason}"
    )

case class AssetBlacklisted(theAsset: IssuedAsset)
    extends MatcherError(asset, commonEntity, blacklisted, e"The asset ${Symbol("assetId") -> theAsset} is blacklisted")

case class AmountAssetBlacklisted(theAsset: IssuedAsset)
    extends MatcherError(asset, amount, blacklisted, e"The amount asset ${Symbol("assetId") -> theAsset} is blacklisted")

case class PriceAssetBlacklisted(theAsset: IssuedAsset)
    extends MatcherError(asset, price, blacklisted, e"The price asset ${Symbol("assetId") -> theAsset} is blacklisted")

case class FeeAssetBlacklisted(theAsset: IssuedAsset)
    extends MatcherError(asset, fee, blacklisted, e"The fee asset ${Symbol("assetId") -> theAsset} is blacklisted")

case class AddressIsBlacklisted(address: Address)
    extends MatcherError(account, commonEntity, blacklisted, e"The account ${Symbol("address") -> address} is blacklisted")

case class BalanceNotEnough(required: List[Amount], actual: List[Amount])
    extends MatcherError(
      account,
      balance,
      notEnough,
      e"Not enough tradable balance. The order requires at least ${Symbol("required") -> required} on balance, but available are ${Symbol("actual") -> actual}"
    )

object BalanceNotEnough {

  def apply(required: Map[Asset, Long], actual: Map[Asset, Long])(implicit efc: ErrorFormatterContext): BalanceNotEnough =
    new BalanceNotEnough(mk(required), mk(actual))

  private def mk(input: Map[Asset, Long])(implicit efc: ErrorFormatterContext): List[Amount] = {
    import Ordered._
    input
      .map { case (id, v) => Amount(id, Denormalization.denormalizeAmountAndFee(v, efc.unsafeAssetDecimals(id))) }
      .toList
      .sortWith((l, r) => l.asset.compatId < r.asset.compatId)
  }

}

case class ActiveOrdersLimitReached(maxActiveOrders: Long)
    extends MatcherError(account, order, limitReached, e"The limit of ${Symbol("limit") -> maxActiveOrders} active orders has been reached")

case class OrderDuplicate(id: Order.Id)
    extends MatcherError(account, order, duplicate, e"The order ${Symbol("id") -> id} has already been placed")

case class OrderNotFound(id: Order.Id) extends MatcherError(order, commonEntity, notFound, e"The order ${Symbol("id") -> id} not found")

case class OrderIsPlacing(id: Order.Id)
    extends MatcherError(order, commonEntity, pending, e"The order ${Symbol("id") -> id} is in the process of placing, please retry later")

case class OrderCanceled(id: Order.Id) extends MatcherError(order, commonEntity, canceled, e"The order ${Symbol("id") -> id} is canceled")

case class OrderFull(id: Order.Id) extends MatcherError(order, commonEntity, limitReached, e"The order ${Symbol("id") -> id} is filled")

case class OrderFinalized(id: Order.Id) extends MatcherError(order, commonEntity, immutable, e"The order ${Symbol("id") -> id} is finalized")

case class OrderVersionUnsupported(version: Byte, requiredFeature: BlockchainFeature)
    extends MatcherError(
      feature,
      order,
      unsupported,
      e"The order of version ${Symbol("version") -> version} isn't yet supported, see the activation status of '${Symbol("featureName") -> requiredFeature}'"
    )

case object RequestInvalidSignature extends MatcherError(request, signature, commonClass, e"The request has an invalid signature")

case class RequestArgumentInvalid(name: String)
    extends MatcherError(request, commonEntity, commonClass, e"The request argument '${Symbol("name") -> name}' is invalid")

case class AccountFeatureUnsupported(x: BlockchainFeature)
    extends MatcherError(
      feature,
      account,
      unsupported,
      e"An account's feature isn't yet supported, see the activation status of '${Symbol("featureName") -> x}'"
    )

case class AccountNotSupportOrderVersion(address: Address, requiredVersion: Byte, givenVersion: Byte)
    extends MatcherError(
      account,
      order,
      unsupported,
      e"The account ${Symbol("address") -> address} requires the version >= ${Symbol("required") -> requiredVersion}, but given ${Symbol("given") -> givenVersion}"
    )

case class AccountScriptReturnedError(address: Address, scriptMessage: String)
    extends MatcherError(
      account,
      script,
      commonClass,
      e"The account's script of ${Symbol("address") -> address} returned the error: ${Symbol("scriptError") -> scriptMessage}"
    )

case class AccountScriptDeniedOrder(address: Address)
    extends MatcherError(account, script, denied, e"The account's script of ${Symbol("address") -> address} rejected the order")

case class AccountScriptUnexpectResult(address: Address, returnedObject: String)
    extends MatcherError(
      account,
      script,
      unexpected,
      e"""The account's script of ${Symbol("address") -> address} is broken, please contact with the owner.
                     |The returned object is '${Symbol("returnedObject") -> returnedObject}'"""
    )

case class AccountScriptException(address: Address, errorName: String, errorText: String)
    extends MatcherError(
      account,
      script,
      broken,
      e"""The account's script of ${Symbol("address") -> address} is broken, please contact with the owner.
                     |The returned error is ${Symbol("errorName") -> errorName}, the text is: ${Symbol("errorText") -> errorText}"""
    )

case class AssetFeatureUnsupported(x: BlockchainFeature, theAsset: IssuedAsset)
    extends MatcherError(
      feature,
      asset,
      unsupported,
      e"""An asset's feature isn't yet supported for '${Symbol("assetId") -> theAsset}',
                     |see the activation status of '${Symbol("featureName") -> x.description}'"""
    )

case class AssetScriptReturnedError(theAsset: IssuedAsset, scriptMessage: String)
    extends MatcherError(
      asset,
      script,
      commonClass,
      e"The asset's script of ${Symbol("assetId") -> theAsset} returned the error: ${Symbol("scriptError") -> scriptMessage}"
    )

case class AssetScriptDeniedOrder(theAsset: IssuedAsset)
    extends MatcherError(asset, script, denied, e"The asset's script of ${Symbol("assetId") -> theAsset} rejected the order")

case class AssetScriptUnexpectResult(theAsset: IssuedAsset, returnedObject: String)
    extends MatcherError(
      asset,
      script,
      unexpected,
      e"""The asset's script of ${Symbol("assetId") -> theAsset} is broken, please contact with the owner.
                   |The returned object is '${Symbol("returnedObject") -> returnedObject}'"""
    )

case class AssetScriptException(theAsset: IssuedAsset, errorName: String, errorText: String)
    extends MatcherError(
      asset,
      script,
      broken,
      e"""The asset's script of ${Symbol("assetId") -> theAsset} is broken, please contact with the owner.
                   |The returned error is ${Symbol("errorName") -> errorName}, the text is: ${Symbol("errorText") -> errorText}"""
    )

case class DeviantOrderPrice(orderType: OrderType, orderPrice: Price, deviationSettings: DeviationsSettings)
    extends MatcherError(
      order,
      price,
      outOfBound,
      if (orderType == OrderType.BUY)
        e"""The buy order's price ${Symbol("price") -> orderPrice} is out of deviation bounds. It should meet the following matcher's requirements:
         |${Symbol("bestBidPercent") -> (100 - deviationSettings.maxPriceProfit)}% of best bid price <= order price <=
         |${Symbol("bestAskPercent") -> (100 + deviationSettings.maxPriceLoss)}% of best ask price"""
      else
        e"""The sell order's price ${Symbol("price") -> orderPrice} is out of deviation bounds. It should meet the following matcher's requirements:
           |${Symbol("bestBidPercent") -> (100 - deviationSettings.maxPriceLoss)}% of best bid price <= order price <=
           |${Symbol("bestAskPercent") -> (100 + deviationSettings.maxPriceProfit)}% of best ask price"""
    )

object DeviantOrderPrice {

  def apply(ord: Order, deviationSettings: DeviationsSettings)(implicit efc: ErrorFormatterContext): DeviantOrderPrice =
    DeviantOrderPrice(ord.orderType, Price(ord.assetPair, ord.price), deviationSettings)

}

case class DeviantOrderMatcherFee(orderType: OrderType, matcherFee: Amount, deviationSettings: DeviationsSettings)
    extends MatcherError(
      order,
      fee,
      outOfBound,
      if (orderType == OrderType.BUY)
        e"""The buy order's matcher fee ${Symbol("matcherFee") -> matcherFee} is out of deviation bounds. It should meet the following matcher's requirements:
       |matcher fee >= ${Symbol(
          "bestAskFeePercent"
        ) -> (100 - deviationSettings.maxFeeDeviation)}% of fee which should be paid in case of matching with best ask"""
      else
        e"""The sell order's matcher fee ${Symbol("matcherFee") -> matcherFee} is out of deviation bounds. It should meet the following matcher's requirements:
         |matcher fee >= ${Symbol(
          "bestBidFeePercent"
        ) -> (100 - deviationSettings.maxFeeDeviation)}% of fee which should be paid in case of matching with best bid"""
    )

object DeviantOrderMatcherFee {

  def apply(ord: Order, deviationSettings: DeviationsSettings)(implicit efc: ErrorFormatterContext): DeviantOrderMatcherFee =
    DeviantOrderMatcherFee(ord.orderType, Amount(ord.feeAsset, ord.matcherFee), deviationSettings)

}

case class AssetPairSameAssets(theAsset: Asset)
    extends MatcherError(
      order,
      assetPair,
      duplicate,
      e"The amount and price assets must be different, but they are: ${Symbol("assetId") -> theAsset}"
    )

case class AssetPairIsDenied(theAssetPair: AssetPair)
    extends MatcherError(order, assetPair, denied, e"Trading is denied for the ${Symbol("assetPair") -> theAssetPair} asset pair")

case class OrderAssetPairReversed(theAssetPair: AssetPair)
    extends MatcherError(order, assetPair, unsupported, e"The ${Symbol("assetPair") -> theAssetPair} asset pair should be reversed")

case class OrderVersionDenied(theVersion: Byte, allowedVersions: Set[Byte])
    extends MatcherError(
      order,
      version,
      denied,
      e"""The orders of version ${Symbol("version") -> theVersion} are denied by matcher.
                     |Allowed order versions are: ${Symbol("allowedOrderVersions") -> allowedVersions.toList.sorted}"""
    )

case class UnsupportedOrderVersion(theVersion: Byte)
    extends MatcherError(
      order,
      version,
      unsupported,
      e"""The orders of version ${Symbol("version") -> theVersion} is not supported.
                     |Supported order versions can be obtained via /matcher/settings GET method"""
    )

case class OrderInvalidAmount(orderAmount: Amount, amtSettings: OrderRestrictionsSettings)
    extends MatcherError(
      order,
      amount,
      denied,
      e"""The order's amount
                     |${Symbol("amount") -> orderAmount}
                     |does not meet matcher's requirements:
                     |max amount = ${Symbol("max") -> amtSettings.maxAmount},
                     |min amount = ${Symbol("min") -> amtSettings.minAmount},
                     |step amount = ${Symbol("step") -> amtSettings.stepAmount}"""
    )

object OrderInvalidAmount {

  def apply(ord: Order, amtSettings: OrderRestrictionsSettings)(implicit efc: ErrorFormatterContext): OrderInvalidAmount =
    OrderInvalidAmount(Amount(ord.assetPair.amountAsset, ord.amount), amtSettings)

}

case class PriceLastDecimalsMustBeZero(insignificantDecimals: Int)
    extends MatcherError(
      order,
      price,
      unexpected,
      e"Invalid price, last ${Symbol("insignificantDecimals") -> insignificantDecimals} digits must be 0"
    )

case class OrderInvalidPrice(orderPrice: Price, prcSettings: OrderRestrictionsSettings)
    extends MatcherError(
      order,
      price,
      denied,
      e"""The order's price
                   |${Symbol("price") -> orderPrice}
                   |does not meet matcher's requirements:
                   |max price = ${Symbol("max") -> prcSettings.maxPrice},
                   |min price = ${Symbol("min") -> prcSettings.minPrice},
                   |step price = ${Symbol("step") -> prcSettings.stepPrice}"""
    )

object OrderInvalidPrice {

  def apply(ord: Order, prcSettings: OrderRestrictionsSettings)(implicit efc: ErrorFormatterContext): OrderInvalidPrice =
    OrderInvalidPrice(Price(ord.assetPair, ord.price), prcSettings)

}

case class MarketOrderCancel(id: Order.Id)
    extends MatcherError(marketOrder, commonEntity, disabled, e"The market order ${Symbol("id") -> id} cannot be cancelled manually")

case class InvalidMarketOrderPrice(orderType: OrderType, orderPrice: Price)
    extends MatcherError(
      marketOrder,
      price,
      outOfBound,
      if (orderType == OrderType.BUY)
        e"""Price of the buy market order
         |(${Symbol("orderPrice") -> orderPrice})
         |is too low for its full execution with the current market state"""
      else
        e"""Price of the sell market order
         |(${Symbol("orderPrice") -> orderPrice})
         |is too high for its full execution with the current market state"""
    )

object InvalidMarketOrderPrice {

  def apply(mo: Order)(implicit efc: ErrorFormatterContext): InvalidMarketOrderPrice =
    InvalidMarketOrderPrice(mo.orderType, Price(mo.assetPair, mo.price))

}

case class OrderInvalidPriceLevel(orderPrice: Price, minOrderPrice: Price)
    extends MatcherError(
      order,
      price,
      notEnough,
      e"""The buy order's price
       |${Symbol("price") -> orderPrice}
       |does not meet matcher's requirements:
       |price >= ${Symbol("tickSize") -> minOrderPrice} (actual tick size).
       |Orders can not be placed into level with price 0"""
    )

object OrderInvalidPriceLevel {

  def apply(ord: Order, tickSize: Long)(implicit efc: ErrorFormatterContext): OrderInvalidPriceLevel =
    OrderInvalidPriceLevel(Price(ord.assetPair, ord.price), Price(ord.assetPair, tickSize))

}

case object WavesNodeConnectionBroken
    extends MatcherError(connectivity, commonEntity, broken, e"Waves Node is unavailable, please retry later or contact with the administrator")

case object UnexpectedError
    extends MatcherError(
      commonEntity,
      commonEntity,
      unexpected,
      e"An unexpected error occurred"
    )

case object WavesImmutableRate
    extends MatcherError(rate, commonEntity, immutable, e"The rate for ${Symbol("assetId") -> (Waves: Asset)} cannot be changed")

case object InvalidAssetRate extends MatcherError(rate, commonEntity, outOfBound, e"Asset rate should be positive and should fit into double")

case class RateNotFound(theAsset: Asset)
    extends MatcherError(rate, commonEntity, notFound, e"The rate for the asset ${Symbol("assetId") -> theAsset} was not specified")

case class InvalidJson(fields: List[String])
    extends MatcherError(
      request,
      commonEntity,
      broken,
      if (fields.isEmpty) e"The provided JSON is invalid. Check the documentation"
      else e"The provided JSON contains invalid fields: ${Symbol("invalidFields") -> fields}. Check the documentation"
    )

case object UnsupportedContentType
    extends MatcherError(request, commonEntity, unsupported, e"The provided Content-Type is not supported, please provide JSON")

case object ApiKeyIsNotProvided
    extends MatcherError(auth, commonEntity, notProvided, e"API key is not provided in the configuration, please contact with the administrator")

case object ApiKeyIsNotValid extends MatcherError(auth, commonEntity, commonClass, e"Provided API key is not correct")

case class UserPublicKeyIsNotValid(reason: String = "invalid public key")
    extends MatcherError(account, pubKey, broken, e"Provided public key is not correct, reason: ${Symbol("reason") -> reason}")

case class InvalidBase58String(reason: String)
    extends MatcherError(order, commonEntity, broken, e"Provided value is not a correct base58 string, reason: ${Symbol("reason") -> reason}")

case class AddressAndPublicKeyAreIncompatible(address: Address, publicKey: PublicKey)
    extends MatcherError(
      auth,
      pubKey,
      unexpected,
      e"Address ${Symbol("address") -> address} and public key ${Symbol("publicKey") -> publicKey} are incompatible"
    )

case object AuthIsRequired extends MatcherError(auth, params, notProvided, e"The authentication is required. Please read the documentation")

case object WsConnectionPongTimeout extends MatcherError(webSocket, connectivity, timedOut, e"WebSocket has reached pong timeout")

case object WsConnectionMaxLifetimeExceeded
    extends MatcherError(webSocket, connectivity, limitReached, e"WebSocket has reached max allowed lifetime")

case class SubscriptionAuthTypeUnsupported(required: Set[String], given: String)
    extends MatcherError(
      auth,
      tpe,
      unsupported,
      e"The subscription authentication type '${Symbol("given") -> given}' is not supported. Required one of: ${Symbol("required") -> required}"
    )

case class JwtCommonError(text: String)
    extends MatcherError(token, commonEntity, commonClass, e"JWT parsing and validation failed: ${Symbol("message") -> text}")

case object JwtBroken extends MatcherError(token, commonEntity, broken, e"JWT has invalid format")

case object JwtPayloadBroken extends MatcherError(token, payload, broken, e"JWT payload has not expected fields")

case object InvalidJwtPayloadSignature extends MatcherError(token, signature, broken, e"The token payload signature is invalid")

case class SubscriptionTokenExpired(address: Address)
    extends MatcherError(token, expiration, commonClass, e"The subscription token for address ${Symbol("address") -> address} expired")

case class TokenNetworkUnexpected(required: Byte, given: Byte)
    extends MatcherError(
      token,
      network,
      unexpected,
      e"The required network is ${Symbol("required") -> required}, but given ${Symbol("given") -> given}"
    )

case class SubscriptionsLimitReached(limit: Int, id: String)
    extends MatcherError(
      webSocket,
      subscription,
      limitReached,
      e"The limit of ${Symbol("limit") -> limit} subscriptions of this type was reached. The subscription of ${Symbol("id") -> id} was stopped"
    )

case class InvalidAddress(reason: String)
    extends MatcherError(address, commonEntity, commonClass, e"Provided address in not correct, reason: ${Symbol("reason") -> reason}")

case class InvalidDepth(reason: String)
    extends MatcherError(request, depth, commonClass, e"Provided depth in not correct, reason: ${Symbol("reason") -> reason}")

sealed abstract class Entity(val code: Int)

// noinspection ScalaStyle
object Entity {
  object common extends Entity(0)
  object request extends Entity(1)
  object feature extends Entity(2)
  object account extends Entity(3)
  object address extends Entity(4)

  object exchangeTx extends Entity(5)

  object balance extends Entity(6)
  object script extends Entity(7)

  object orderBook extends Entity(8)
  object order extends Entity(9)

  object version extends Entity(10)
  object asset extends Entity(11)
  object pubKey extends Entity(12)
  object signature extends Entity(13)
  object assetPair extends Entity(14)
  object amount extends Entity(15)
  object price extends Entity(16)
  object fee extends Entity(17)
  object expiration extends Entity(18)
  object marketOrder extends Entity(19)
  object rate extends Entity(20)
  object tpe extends Entity(21)
  object network extends Entity(22)

  object producer extends Entity(100)
  object connectivity extends Entity(101)
  object auth extends Entity(102)
  object params extends Entity(103)
  object webSocket extends Entity(104)
  object token extends Entity(105)
  object payload extends Entity(106)
  object subscription extends Entity(107)
  object depth extends Entity(108)
}

sealed abstract class Class(val code: Int)

// noinspection ScalaStyle
object Class {
  object common extends Class(0)
  object broken extends Class(1)
  object denied extends Class(2)
  object unsupported extends Class(3)
  object unexpected extends Class(4)
  object blacklisted extends Class(5)
  object notEnough extends Class(6)
  object limitReached extends Class(7)
  object duplicate extends Class(8)
  object notFound extends Class(9)
  object canceled extends Class(10)
  object immutable extends Class(11)
  object timedOut extends Class(12)
  object starting extends Class(13)
  object stopping extends Class(14)
  object outOfBound extends Class(15)
  object disabled extends Class(16)
  object notProvided extends Class(17)
  object optimization extends Class(18)
  object pending extends Class(19)
}
