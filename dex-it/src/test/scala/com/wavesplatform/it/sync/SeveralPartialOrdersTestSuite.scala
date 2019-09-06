package com.wavesplatform.it.sync

import com.wavesplatform.it.NewMatcherSuiteBase
import com.wavesplatform.it.api.{LevelResponse, OrderStatus}
import com.wavesplatform.it.config.DexTestConfig._
import com.wavesplatform.transaction.Asset.Waves
import com.wavesplatform.transaction.assets.exchange.OrderType.BUY
import com.wavesplatform.transaction.assets.exchange.{Order, OrderType}

import scala.math.BigDecimal.RoundingMode

class SeveralPartialOrdersTestSuite extends NewMatcherSuiteBase {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    broadcast(IssueUsdTx)
  }

  "Alice and Bob trade WAVES-USD" - {
    val price           = 238
    val buyOrderAmount  = 425532L
    val sellOrderAmount = 840340L

    "place usd-waves order" in {
      // Alice wants to sell USD for Waves
      val bobWavesBalanceBefore = wavesNode1Api.balance(bob, Waves)

      val bobOrder1 = mkOrder(bob, wavesUsdPair, OrderType.SELL, sellOrderAmount, price)
      dex1Api.place(bobOrder1)
      dex1Api.waitForOrderStatus(bobOrder1, OrderStatus.Accepted)
      dex1Api.reservedBalance(bob)(Waves) shouldBe sellOrderAmount + matcherFee
      dex1Api.tradableBalance(bob, wavesUsdPair)(Waves) shouldBe bobWavesBalanceBefore - (sellOrderAmount + matcherFee)

      val aliceOrder = mkOrder(alice, wavesUsdPair, OrderType.BUY, buyOrderAmount, price)
      dex1Api.place(aliceOrder)
      dex1Api.waitForOrderStatus(aliceOrder, OrderStatus.Filled)

      val aliceOrder2 = mkOrder(alice, wavesUsdPair, OrderType.BUY, buyOrderAmount, price)
      dex1Api.place(aliceOrder2)
      dex1Api.waitForOrderStatus(aliceOrder2, OrderStatus.Filled)

      // Bob wants to buy some USD
      dex1Api.waitForOrderStatus(bobOrder1, OrderStatus.Filled)

      // Each side get fair amount of assets
      waitForOrderAtNode(bobOrder1.id())
      dex1Api.reservedBalance(bob) shouldBe empty
      dex1Api.reservedBalance(alice) shouldBe empty

      // Previously cancelled order should not affect new orders
      val orderBook1 = dex1Api.orderBook(wavesUsdPair)
      orderBook1.asks shouldBe empty
      orderBook1.bids shouldBe empty

      val bobOrder2 = mkOrder(bob, wavesUsdPair, OrderType.SELL, sellOrderAmount, price)
      dex1Api.place(bobOrder2)
      dex1Api.waitForOrderStatus(bobOrder2, OrderStatus.Accepted)

      val orderBook2 = dex1Api.orderBook(wavesUsdPair)
      orderBook2.asks shouldBe List(LevelResponse(bobOrder2.amount, bobOrder2.price))
      orderBook2.bids shouldBe empty

      dex1Api.cancel(bob, bobOrder2)
      dex1Api.waitForOrderStatus(bobOrder2, OrderStatus.Cancelled)

      dex1Api.reservedBalance(bob) shouldBe empty
      dex1Api.reservedBalance(alice) shouldBe empty
    }

    "place one submitted orders and two counter" in {
      val aliceOrder1 = mkOrder(alice, wavesUsdPair, OrderType.BUY, buyOrderAmount, price)
      dex1Api.place(aliceOrder1)

      val aliceOrder2 = mkOrder(alice, wavesUsdPair, OrderType.BUY, buyOrderAmount, price)
      dex1Api.place(aliceOrder2)

      val bobOrder1 = mkOrder(bob, wavesUsdPair, OrderType.SELL, sellOrderAmount, price)
      dex1Api.place(bobOrder1)

      dex1Api.waitForOrderStatus(aliceOrder1, OrderStatus.Filled)
      dex1Api.waitForOrderStatus(aliceOrder2, OrderStatus.Filled)
      dex1Api.waitForOrderStatus(bobOrder1, OrderStatus.Filled)

      // Each side get fair amount of assets
      waitForOrderAtNode(bobOrder1.id())
      dex1Api.reservedBalance(bob) shouldBe empty
      dex1Api.reservedBalance(alice) shouldBe empty

      // Previously cancelled order should not affect new orders
      val orderBook1 = dex1Api.orderBook(wavesUsdPair)
      orderBook1.asks shouldBe empty
      orderBook1.bids shouldBe empty

      val bobOrder2 = mkOrder(bob, wavesUsdPair, OrderType.SELL, sellOrderAmount, price)
      dex1Api.place(bobOrder2)
      dex1Api.waitForOrderStatus(bobOrder2, OrderStatus.Accepted)

      val orderBook2 = dex1Api.orderBook(wavesUsdPair)
      orderBook2.asks shouldBe List(LevelResponse(bobOrder2.amount, bobOrder2.price))
      orderBook2.bids shouldBe empty
    }
  }

  private def correctAmount(a: Long, price: Long): Long = {
    val settledTotal = (BigDecimal(price) * a / Order.PriceConstant).setScale(0, RoundingMode.FLOOR).toLong
    (BigDecimal(settledTotal) / price * Order.PriceConstant).setScale(0, RoundingMode.CEILING).toLong
  }

  private def receiveAmount(ot: OrderType, matchAmount: Long, matchPrice: Long): Long =
    if (ot == BUY) correctAmount(matchAmount, matchPrice)
    else (BigInt(matchAmount) * matchPrice / Order.PriceConstant).bigInteger.longValueExact()
}
