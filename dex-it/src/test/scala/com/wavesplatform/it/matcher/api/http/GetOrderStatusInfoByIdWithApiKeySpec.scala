package com.wavesplatform.it.matcher.api.http

import com.softwaremill.sttp.StatusCodes
import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.dex.api.http.entities.HttpOrderBookHistoryItem
import com.wavesplatform.dex.domain.account.KeyPair.toAddress
import com.wavesplatform.dex.domain.bytes.codec.Base58
import com.wavesplatform.dex.domain.order.Order
import com.wavesplatform.dex.domain.order.OrderType.{BUY, SELL}
import com.wavesplatform.dex.it.api.RawHttpChecks
import com.wavesplatform.dex.it.docker.apiKey
import com.wavesplatform.dex.model.{AcceptedOrderType, OrderStatus}
import com.wavesplatform.it.MatcherSuiteBase
import org.scalatest.prop.TableDrivenPropertyChecks

class GetOrderStatusInfoByIdWithApiKeySpec extends MatcherSuiteBase with TableDrivenPropertyChecks with RawHttpChecks {

  override protected def dexInitialSuiteConfig: Config = ConfigFactory.parseString(
    s"""waves.dex {
       |  price-assets = [ "$BtcId", "$UsdId", "WAVES" ]
       |}""".stripMargin
  )

  val order = mkOrder(alice, wavesUsdPair, BUY, 10.waves, 2.usd)

  override protected def beforeAll(): Unit = {
    wavesNode1.start()
    broadcastAndAwait(IssueBtcTx, IssueUsdTx)
    dex1.start()
  }

  "GET /matcher/orders/{address}/{orderId} " - {

    def httpOrderInfo(o: Order, s: OrderStatus, f: Long = 0.003.waves, avgWeighedPrice: Long = 0, totalExecutedPriceAssets: Long = 0): HttpOrderBookHistoryItem =
      HttpOrderBookHistoryItem(
        o.id(),
        o.orderType,
        AcceptedOrderType.Limit,
        o.amount,
        s.filledAmount,
        o.price,
        f,
        s.filledFee,
        o.feeAsset,
        o.timestamp,
        s.name,
        o.assetPair,
        avgWeighedPrice,
        o.version,
        totalExecutedPriceAssets
      )

    "should return correct status of the order" in {
      val o = mkOrder(alice, wavesUsdPair, BUY, 10.waves, 2.usd)
      placeAndAwaitAtDex(o)

      withClue(" - accepted") {
        validate200Json(dex1.rawApi.getOrderStatusInfoByIdWithApiKey(alice, o.id(), Some(alice.publicKey))) should matchTo(httpOrderInfo(
          o,
          OrderStatus.Accepted
        ))
      }

      withClue(" - partially filled") {
        placeAndAwaitAtNode(mkOrder(alice, wavesUsdPair, SELL, 5.waves, 2.usd))

        validate200Json(dex1.rawApi.getOrderStatusInfoByIdWithApiKey(alice, o.id(), Some(alice.publicKey))) should matchTo(httpOrderInfo(
          o,
          OrderStatus.PartiallyFilled(5.waves, 0.0015.waves),
          avgWeighedPrice = 2.usd,
          totalExecutedPriceAssets = 10.usd
        ))
      }

      withClue(" - filled") {
        placeAndAwaitAtNode(mkOrder(alice, wavesUsdPair, SELL, 5.waves, 2.usd))

        validate200Json(dex1.rawApi.getOrderStatusInfoByIdWithApiKey(alice, o.id(), Some(alice.publicKey))) should matchTo(httpOrderInfo(
          o,
          OrderStatus.Filled(10.waves, 0.003.waves),
          avgWeighedPrice = 2.usd,
          totalExecutedPriceAssets = 20.usd
        ))
      }

      withClue(" - cancelled") {
        val o = mkOrder(alice, wavesUsdPair, BUY, 10.waves, 2.usd)
        placeAndAwaitAtDex(o)
        cancelAndAwait(alice, o)

        validate200Json(dex1.rawApi.getOrderStatusInfoByIdWithApiKey(alice, o.id(), Some(alice.publicKey))) should matchTo(httpOrderInfo(
          o,
          OrderStatus.Cancelled(0, 0)
        ))
      }
    }

    "should return an error when without headers" in {
      val order = mkOrder(alice, wavesUsdPair, BUY, 10.waves, 2.usd)
      placeAndAwaitAtDex(order)
      validateAuthorizationError(
        dex1.rawApi.getOrderStatusInfoById(alice.toAddress.stringRepr, order.idStr())
      )
    }

    "should return an error when the public key header is not of order owner" in {
      val order = mkOrder(alice, wavesUsdPair, BUY, 10.waves, 2.usd)
      placeAndAwaitAtDex(order)
      validateMatcherError(
        dex1.rawApi.getOrderStatusInfoByIdWithApiKey(alice, order.id(), Some(bob.publicKey)),
        StatusCodes.Forbidden,
        3148801,
        "Provided user public key is not correct"
      )
    }

    "should return an error when the public api-key header is not correct" in {
      val order = mkOrder(alice, wavesUsdPair, BUY, 10.waves, 2.usd)
      placeAndAwaitAtDex(order)

      validateAuthorizationError(
        dex1.rawApi.getOrderStatusInfoById(
          alice.toAddress.stringRepr,
          order.idStr(),
          Map("X-User-Public-Key" -> Base58.encode(alice.publicKey), "X-API-Key" -> "incorrect")
        )
      )
    }

    "should return an error when the order doesn't exist" in {
      val order = mkOrder(alice, wavesUsdPair, BUY, 10.waves, 2.usd)
      validateMatcherError(
        dex1.rawApi.getOrderStatusInfoByIdWithApiKey(alice, order.id(), Some(alice.publicKey)),
        StatusCodes.NotFound,
        9437193,
        s"The order ${order.idStr()} not found"
      )
    }

    "should return an error when address is not a correct base58 string" in {
      val order = mkOrder(alice, wavesUsdPair, BUY, 10.waves, 2.usd)
      validateMatcherError(
        dex1.rawApi.getOrderStatusInfoById(
          "null",
          order.idStr(),
          Map("X-User-Public-Key" -> Base58.encode(alice.publicKey), "X-API-Key" -> apiKey)
        ),
        StatusCodes.BadRequest,
        4194304,
        "Provided address in not correct, reason: Unable to decode base58: requirement failed: Wrong char 'l' in Base58 string 'null'"
      )
    }

    //TODO: change after DEX-980
    "should return an error when orderId is not a correct base58 string" in {
      validate404Exception(
        dex1.rawApi.getOrderStatusInfoById(
          alice.toAddress.stringRepr,
          "null",
          Map("X-User-Public-Key" -> Base58.encode(alice.publicKey), "X-API-Key" -> apiKey)
        )
      )
    }

  }
}
