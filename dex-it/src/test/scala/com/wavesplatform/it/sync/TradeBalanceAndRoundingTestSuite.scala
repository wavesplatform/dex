package com.wavesplatform.it.sync

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.dex.api.http.entities.HttpOrderStatus.Status
import com.wavesplatform.dex.api.http.entities.{HttpAssetInfo, HttpOrderStatus}
import com.wavesplatform.dex.domain.asset.Asset.Waves
import com.wavesplatform.dex.domain.order.OrderType.{BUY, SELL}
import com.wavesplatform.dex.domain.order.{Order, OrderType}
import com.wavesplatform.dex.model.AcceptedOrder
import com.wavesplatform.it.MatcherSuiteBase

import scala.math.BigDecimal.RoundingMode

class TradeBalanceAndRoundingTestSuite extends MatcherSuiteBase {

  override protected def dexInitialSuiteConfig: Config = ConfigFactory.parseString(s"""waves.dex.price-assets = [ "$UsdId", "WAVES" ]""")

  override protected def beforeAll(): Unit = {
    wavesNode1.start()
    broadcastAndAwait(IssueUsdTx, IssueEthTx, IssueWctTx)
    dex1.start()
  }

  "Alice and Bob trade WAVES-USD" - {
    val price = 238
    val buyOrderAmount = 425532L
    val sellOrderAmount = 3100000000L

    val correctedSellAmount = correctAmount(sellOrderAmount, price)

    val adjustedAmount = receiveAmount(OrderType.BUY, buyOrderAmount, price)
    val adjustedTotal = receiveAmount(OrderType.SELL, buyOrderAmount, price)

    var aliceWavesBalanceBefore = 0L
    var bobWavesBalanceBefore = 0L

    "prepare" in {
      log.debug(s"correctedSellAmount: $correctedSellAmount, adjustedAmount: $adjustedAmount, adjustedTotal: $adjustedTotal")
      aliceWavesBalanceBefore = wavesNode1.api.balance(alice, Waves)
      bobWavesBalanceBefore = wavesNode1.api.balance(bob, Waves)
    }

    "place usd-waves order" in {
      // Alice wants to sell USD for Waves
      val bobOrder1 = mkOrder(bob, wavesUsdPair, OrderType.SELL, sellOrderAmount, price)
      placeAndAwaitAtDex(bobOrder1)
      dex1.api.getReservedBalance(bob)(Waves) shouldBe sellOrderAmount + matcherFee
      dex1.api.getTradableBalance(bob, wavesUsdPair)(Waves) shouldBe bobWavesBalanceBefore - (sellOrderAmount + matcherFee)

      val aliceOrder = mkOrder(alice, wavesUsdPair, OrderType.BUY, buyOrderAmount, price)
      dex1.api.place(aliceOrder)
      dex1.api.waitForOrder(aliceOrder)(_ == HttpOrderStatus(Status.Filled, filledAmount = Some(420169L), filledFee = Some(296219L)))

      // Bob wants to buy some USD
      dex1.api.waitForOrder(bobOrder1)(_ == HttpOrderStatus(Status.PartiallyFilled, filledAmount = Some(420169L), filledFee = Some(40L)))

      // Each side get fair amount of assets
      waitForOrderAtNode(aliceOrder)
    }

    "get opened trading markets. USD price-asset" in {
      val openMarkets = dex1.api.getOrderBooks
      openMarkets.markets.size shouldBe 1
      val markets = openMarkets.markets.head

      markets.amountAssetName shouldBe "WAVES"
      markets.amountAssetInfo shouldBe Some(HttpAssetInfo(8))

      markets.priceAssetName shouldBe usdAssetName
      markets.priceAssetInfo shouldBe Some(HttpAssetInfo(IssueUsdTx.decimals()))
    }

    "check usd and waves balance after fill" in {
      val aliceWavesBalanceAfter = wavesNode1.api.balance(alice, Waves)
      val aliceUsdBalance = wavesNode1.api.balance(alice, usd)

      val bobWavesBalanceAfter = wavesNode1.api.balance(bob, Waves)
      val bobUsdBalance = wavesNode1.api.balance(bob, usd)

      (aliceWavesBalanceAfter - aliceWavesBalanceBefore) should be(
        adjustedAmount - (BigInt(matcherFee) * adjustedAmount / buyOrderAmount).bigInteger.longValue()
      )

      aliceUsdBalance - defaultAssetQuantity should be(-adjustedTotal)
      bobWavesBalanceAfter - bobWavesBalanceBefore should be(
        -adjustedAmount - (BigInt(matcherFee) * adjustedAmount / sellOrderAmount).bigInteger.longValue()
      )
      bobUsdBalance should be(adjustedTotal)
    }

    "check filled amount and tradable balance" in {
      val bobOrder = dex1.api.orderHistory(bob).head
      val filledAmount = dex1.api.getOrderStatus(bobOrder.assetPair, bobOrder.id).filledAmount.getOrElse(0L)

      filledAmount shouldBe adjustedAmount
    }

    "check reserved balance" in {
      val reservedFee = BigInt(matcherFee) - (BigInt(matcherFee) * adjustedAmount / sellOrderAmount)
      log.debug(s"reservedFee: $reservedFee")
      val expectedBobReservedBalance = correctedSellAmount - adjustedAmount + reservedFee
      dex1.api.getReservedBalance(bob)(Waves) shouldBe expectedBobReservedBalance

      dex1.api.getReservedBalance(alice) shouldBe empty
    }

    "check waves-usd tradable balance" in {
      val orderHistory = dex1.api.orderHistory(bob)
      orderHistory.size should be(1)

      val expectedBobTradableBalance = bobWavesBalanceBefore - (correctedSellAmount + matcherFee)
      dex1.api.getTradableBalance(bob, wavesUsdPair)(Waves) shouldBe expectedBobTradableBalance
      dex1.api.getTradableBalance(alice, wavesUsdPair)(Waves) shouldBe wavesNode1.api.balance(alice, Waves)

      val order = orderHistory.head
      dex1.api.cancel(bob, order.assetPair, order.id)
      dex1.api.waitForOrderStatus(order.assetPair, order.id, Status.Cancelled)
      dex1.api.getTradableBalance(bob, order.assetPair)(Waves) shouldBe wavesNode1.api.balance(bob, Waves)
    }
  }

  "Alice and Bob trade WAVES-USD check CELLING" - {
    val price2 = 289
    val buyOrderAmount2 = 0.07.waves
    val sellOrderAmount2 = 3.waves

    val correctedSellAmount2 = correctAmount(sellOrderAmount2, price2)

    "place usd-waves order" in {
      // Alice wants to sell USD for Waves
      val bobWavesBalanceBefore = wavesNode1.api.balance(bob, Waves)
      dex1.api.getTradableBalance(bob, wavesUsdPair)(Waves)
      val bobOrder1 = mkOrder(bob, wavesUsdPair, OrderType.SELL, sellOrderAmount2, price2)
      placeAndAwaitAtDex(bobOrder1)

      dex1.api.getReservedBalance(bob)(Waves) shouldBe correctedSellAmount2 + matcherFee
      dex1.api.getTradableBalance(bob, wavesUsdPair)(Waves) shouldBe bobWavesBalanceBefore - (correctedSellAmount2 + matcherFee)

      val aliceOrder = mkOrder(alice, wavesUsdPair, OrderType.BUY, buyOrderAmount2, price2)
      placeAndAwaitAtDex(aliceOrder, Status.Filled)

      // Bob wants to buy some USD
      dex1.api.waitForOrderStatus(bobOrder1, Status.PartiallyFilled)

      // Each side get fair amount of assets
      waitForOrderAtNode(aliceOrder)
      dex1.api.cancel(bob, bobOrder1)
    }

  }

  "Alice and Bob trade WCT-USD sell price less than buy price" - {
    "place wcd-usd order corrected by new price sell amount less then initial one" in {
      val buyPrice = 247700
      val sellPrice = 135600
      val buyAmount = 46978
      val sellAmount = 56978

      val bobOrder = mkOrder(bob, wctUsdPair, SELL, sellAmount, sellPrice)
      placeAndAwaitAtDex(bobOrder)

      val aliceOrder = mkOrder(alice, wctUsdPair, BUY, buyAmount, buyPrice)
      placeAndAwaitAtDex(aliceOrder, Status.Filled)

      waitForOrderAtNode(aliceOrder)
      dex1.api.cancel(bob, bobOrder)

      dex1.api.waitForOrderStatus(bobOrder, Status.Cancelled)

      dex1.api.getReservedBalance(bob) shouldBe empty
      dex1.api.getReservedBalance(alice) shouldBe empty
    }
  }

  "Alice and Bob trade WCT-USD 1" - {
    val wctUsdSellAmount = 347
    val wctUsdBuyAmount = 146
    val wctUsdPrice = 12739213

    "place wct-usd order" in {
      val aliceUsdBalance = wavesNode1.api.balance(alice, usd)
      val bobUsdBalance = wavesNode1.api.balance(bob, usd)
      val bobWctInitBalance = wavesNode1.api.balance(bob, wct)

      val bobOrder = mkOrder(bob, wctUsdPair, SELL, wctUsdSellAmount, wctUsdPrice)
      placeAndAwaitAtDex(bobOrder)

      val aliceOrder = mkOrder(alice, wctUsdPair, BUY, wctUsdBuyAmount, wctUsdPrice)
      placeAndAwaitAtDex(aliceOrder, Status.Filled)

      waitForOrderAtNode(aliceOrder)

      val executedAmount = correctAmount(wctUsdBuyAmount, wctUsdPrice) // 142
      val bobReceiveUsdAmount = receiveAmount(SELL, wctUsdBuyAmount, wctUsdPrice)
      val expectedReservedBobWct = wctUsdSellAmount - executedAmount // 205 = 347 - 142

      eventually {
        dex1.api.getReservedBalance(bob)(wct) shouldBe expectedReservedBobWct
        // 999999999652 = 999999999999 - 142 - 205
        dex1.api.getTradableBalance(bob, wctUsdPair)(wct) shouldBe bobWctInitBalance - executedAmount - expectedReservedBobWct
        dex1.api.getTradableBalance(bob, wctUsdPair)(usd) shouldBe bobUsdBalance + bobReceiveUsdAmount
      }

      dex1.api.getReservedBalance(alice) shouldBe empty
      dex1.api.getTradableBalance(alice, wctUsdPair)(usd) shouldBe aliceUsdBalance - bobReceiveUsdAmount

      val expectedReservedWaves = matcherFee - AcceptedOrder.partialFee(matcherFee, wctUsdSellAmount, executedAmount)
      dex1.api.getReservedBalance(bob)(Waves) shouldBe expectedReservedWaves

      dex1.api.cancel(bob, wctUsdPair, dex1.api.orderHistory(bob).head.id)
    }

    "reserved balance is empty after the total execution" in {
      val aliceOrder = mkOrder(alice, wctUsdPair, BUY, 5000000, 100000)
      placeAndAwaitAtDex(aliceOrder)

      val bobOrder = mkOrder(bob, wctUsdPair, SELL, 5000000, 99908)
      placeAndAwaitAtDex(bobOrder, Status.Filled)
      dex1.api.waitForOrderStatus(aliceOrder, Status.Filled)

      waitForOrderAtNode(bobOrder)
      eventually {
        dex1.api.getReservedBalance(alice) shouldBe empty
        dex1.api.getReservedBalance(bob) shouldBe empty
      }
    }
  }

  "get opened trading markets. Check WCT-USD" in {
    val openMarkets = dex1.api.getOrderBooks
    val markets = openMarkets.markets.last

    markets.amountAssetName shouldBe wctAssetName
    markets.amountAssetInfo shouldBe Some(HttpAssetInfo(IssueWctTx.decimals()))

    markets.priceAssetName shouldBe usdAssetName
    markets.priceAssetInfo shouldBe Some(HttpAssetInfo(IssueUsdTx.decimals()))
  }

  "Alice and Bob trade WCT-WAVES on not enough fee when place order" - {
    val wctWavesSellAmount = 2
    val wctWavesPrice = 11234560000000L

    "bob lease all waves exact half matcher fee" in {
      val leasingAmount = wavesNode1.api.balance(bob, Waves) - leasingFee - matcherFee / 2
      val leaseTx = mkLease(bob, matcher, leasingAmount)
      broadcastAndAwait(leaseTx)

      val bobOrder = mkOrder(bob, wctWavesPair, SELL, wctWavesSellAmount, wctWavesPrice)
      dex1.tryApi.place(bobOrder) should failWith(3147270) // BalanceNotEnough

      broadcastAndAwait(mkLeaseCancel(bob, leaseTx.id()))
    }
  }

  "Alice and Bob trade ETH-WAVES" - {
    "reserved balance is empty after the total execution" in {
      val counter1 = mkOrder(alice, ethWavesPair, SELL, 2864310, 300000)
      placeAndAwaitAtDex(counter1)

      val counter2 = mkOrder(alice, ethWavesPair, SELL, 7237977, 300000)
      placeAndAwaitAtDex(counter2)

      val submitted = mkOrder(bob, ethWavesPair, BUY, 4373667, 300000)
      dex1.api.place(submitted)

      dex1.api.waitForOrderStatus(counter1, Status.Filled)
      dex1.api.waitForOrderStatus(counter2, Status.PartiallyFilled)
      dex1.api.waitForOrderStatus(submitted, Status.Filled)

      waitForOrderAtNode(submitted)
      eventually {
        dex1.api.getReservedBalance(bob) shouldBe empty
      }
      dex1.api.cancel(alice, counter2)
    }
  }

  "submitted order is canceled during match" in {

    Seq(alice, bob).foreach(dex1.api.cancelAll(_))

    val bobOrder = mkOrderDP(bob, wavesUsdPair, OrderType.SELL, 0.1.waves, 0.1)
    placeAndAwaitAtDex(bobOrder)

    val aliceOrder = mkOrderDP(alice, wavesUsdPair, OrderType.BUY, 0.001.waves, 10.0)
    dex1.api.place(aliceOrder)
    dex1.api.waitForOrder(aliceOrder)(_ == HttpOrderStatus(Status.Cancelled, filledAmount = Some(0), filledFee = Some(0)))

    withClue("Alice's reserved balance:") {
      dex1.api.getReservedBalance(alice) shouldBe empty
    }

    val aliceOrders = dex1.api.orderHistoryWithApiKey(alice, activeOnly = Some(false))
    aliceOrders should not be empty

    val order = aliceOrders
      .find(_.id == aliceOrder.id())
      .getOrElse(throw new IllegalStateException(s"Alice should have the ${aliceOrder.id()} order"))

    order.status shouldBe Status.Cancelled.name
    dex1.api.cancel(bob, bobOrder)
  }

  private def correctAmount(a: Long, price: Long): Long = {
    val settledTotal = (BigDecimal(price) * a / Order.PriceConstant).setScale(0, RoundingMode.FLOOR).toLong
    (BigDecimal(settledTotal) / price * Order.PriceConstant).setScale(0, RoundingMode.CEILING).toLong
  }

  private def receiveAmount(ot: OrderType, matchAmount: Long, matchPrice: Long): Long =
    if (ot == BUY) correctAmount(matchAmount, matchPrice)
    else (BigInt(matchAmount) * matchPrice / Order.PriceConstant).bigInteger.longValueExact()

}
