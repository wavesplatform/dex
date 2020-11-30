package com.wavesplatform.it.sync

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory.parseString
import com.wavesplatform.dex.api.http.entities.HttpOrderStatus.{apply => _, _}
import com.wavesplatform.dex.domain.account.{Address, KeyPair}
import com.wavesplatform.dex.domain.asset.Asset.IssuedAsset
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.domain.order.Order
import com.wavesplatform.dex.domain.order.OrderType._
import com.wavesplatform.dex.it.api.responses.dex.MatcherError
import com.wavesplatform.it.MatcherSuiteBase
import org.scalatest._

class BlacklistedTradingTestSuite extends MatcherSuiteBase with GivenWhenThen {

  override protected def dexInitialSuiteConfig: Config = configWithBlacklisted()

  override protected def beforeAll(): Unit = {
    wavesNode1.start()
    broadcastAndAwait(IssueUsdTx, IssueWctTx, IssueEthTx, IssueBtcTx)
    dex1.start()
  }

  "When blacklists are empty" in {
    val (dec2, dec8) = (1000L, 1000000000L)

    Then("Place some orders")
    val usdOrder = mkOrder(alice, wavesUsdPair, BUY, dec8, dec2)
    val wctOrder = mkOrder(alice, wctWavesPair, BUY, dec2, dec8)
    val ethOrder = mkOrder(alice, ethWavesPair, SELL, dec8, dec8)
    val btcOrder1 = mkOrder(bob, wavesBtcPair, SELL, dec8, dec8)
    List(usdOrder, wctOrder, ethOrder, btcOrder1).foreach(dex1.api.place)
    dex1.api.waitForOrderStatus(btcOrder1, Status.Accepted)

    Then("We blacklist some assets and addresses and restart the node")
    dex1.restartWithNewSuiteConfig(
      configWithBlacklisted(
        assets = Array(wct),
        names = Array("ETH.*"),
        addresses = Array(bob)
      )
    )

    Then("orders for blacklisted assets are not available and new orders can't be placed")
    testOrderStatusDenied(wctOrder, IssuedAsset(WctId))
    testOrderStatusDenied(ethOrder, IssuedAsset(EthId))

    testOrderPlacementDenied(mkOrder(alice, wctWavesPair, BUY, dec2, dec8), IssuedAsset(WctId))
    testOrderPlacementDenied(mkOrder(alice, ethWavesPair, SELL, dec8, dec8), IssuedAsset(EthId))

    testOrderPlacementDenied(mkOrder(bob, wavesBtcPair, SELL, dec8, dec8), bob)

    And("orders of blacklisted address are still available")
    dex1.api.getOrderStatus(btcOrder1).status shouldBe Status.Accepted

    And("orders for other assets are still available")
    dex1.api.getOrderStatus(usdOrder).status shouldBe Status.Accepted

    And("OrderBook for blacklisted assets is not available")
    testOrderBookDenied(wctWavesPair, IssuedAsset(WctId))
    testOrderBookDenied(ethWavesPair, IssuedAsset(EthId))
    dex1.api.getOrderBook(wavesBtcPair).asks.size shouldBe 1

    And("OrderHistory returns info about all orders")
    val aliceOrderHistory = dex1.api.orderHistory(alice, activeOnly = Some(true))
    aliceOrderHistory.size shouldBe 3
    aliceOrderHistory.foreach(_.status shouldBe Status.Accepted.name)

    val bobOrderHistory = dex1.api.orderHistory(bob, activeOnly = Some(true))
    bobOrderHistory.size shouldBe 1
    bobOrderHistory.head.status shouldBe Status.Accepted.name

    And("Trading markets have info about all asset pairs")
    dex1.api.getOrderBooks.markets.size shouldBe 4

    And("balances are still reserved")
    dex1.api.getReservedBalance(alice).size shouldBe 3
    dex1.api.getReservedBalance(bob).size shouldBe 1

    And("orders for other assets are still available")
    dex1.api.getOrderStatus(usdOrder).status shouldBe Status.Accepted

    And("order can be placed on allowed pair with blacklisted asset")
    val btcOrder2 = mkOrder(alice, wavesBtcPair, SELL, dec8, dec8)
    placeAndAwaitAtDex(btcOrder2)

    And("now if all blacklists are cleared")
    dex1.restartWithNewSuiteConfig(configWithBlacklisted())

    Then("OrderBook for blacklisted assets is available again")
    dex1.api.getOrderBook(wctWavesPair).bids.size shouldBe 1
    dex1.api.getOrderBook(ethWavesPair).asks.size shouldBe 1

    And("order statuses are available again")
    dex1.api.getOrderStatus(wctOrder).status shouldBe Status.Accepted
    dex1.api.getOrderStatus(ethOrder).status shouldBe Status.Accepted

    And("new orders can be placed")
    val newWctOrder = mkOrder(alice, wctWavesPair, BUY, dec2, dec8)
    val newEthOrder = mkOrder(alice, ethWavesPair, SELL, dec8, dec8)
    val btcOrder3 = mkOrder(bob, wavesBtcPair, SELL, dec8, dec8)
    val newOrders = List(newWctOrder, newEthOrder, btcOrder3)
    newOrders.foreach(dex1.api.place)
    newOrders.foreach(dex1.api.waitForOrderStatus(_, Status.Accepted))
  }

  private def testOrderPlacementDenied(order: Order, address: Address): Unit =
    dex1.tryApi.place(order) should failWith(3145733, MatcherError.Params(address = Some(address.stringRepr)))

  private def testOrderPlacementDenied(order: Order, blacklistedAsset: Asset): Unit =
    failedDueAssetBlacklist(dex1.tryApi.place(order), order.assetPair, blacklistedAsset)

  private def testOrderStatusDenied(order: Order, blacklistedAsset: Asset): Unit =
    failedDueAssetBlacklist(dex1.tryApi.getOrderStatus(order), order.assetPair, blacklistedAsset)

  private def testOrderBookDenied(assetPair: AssetPair, blacklistedAsset: Asset): Unit =
    failedDueAssetBlacklist(dex1.tryApi.getOrderBook(assetPair), assetPair, blacklistedAsset)

  private def failedDueAssetBlacklist(r: Either[MatcherError, Any], assetPair: AssetPair, blacklistedAsset: Asset) =
    r should failWith(expectedErrorCode(assetPair, blacklistedAsset), MatcherError.Params(assetId = Some(blacklistedAsset.toString)))

  private def expectedErrorCode(assetPair: AssetPair, blacklistedAsset: Asset): Int =
    if (blacklistedAsset == assetPair.amountAsset) 11538181 // AmountAssetBlacklisted
    else 11538437 // PriceAssetBlacklisted

  private def configWithBlacklisted(
    assets: Array[Asset] = Array.empty,
    names: Array[String] = Array.empty,
    addresses: Array[KeyPair] = Array.empty,
    allowedAssetPairs: Array[String] = Array.empty
  ): Config = {
    def toStr(array: Array[String]): String = if (array.length == 0) "" else array.mkString("\"", "\", \"", "\"")
    parseString(s"""
                   |waves.dex {
                   |  price-assets = [ "$UsdId", "$BtcId", "WAVES" ]
                   |  blacklisted-assets = [${toStr(assets.map(_.toString))}]
                   |  blacklisted-names = [${toStr(names)}]
                   |  blacklisted-addresses = [${toStr(addresses.map(_.toAddress.toString))}]
                   |  allowed-asset-pairs = [${toStr(allowedAssetPairs)}]
                   |  white-list-only = no
                   |}
    """.stripMargin)
  }

}
