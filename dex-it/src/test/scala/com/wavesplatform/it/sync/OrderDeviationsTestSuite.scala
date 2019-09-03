//package com.wavesplatform.it.sync
//
//import com.typesafe.config.{Config, ConfigFactory}
//import com.wavesplatform.it.MatcherSuiteBase
//import com.wavesplatform.it.api.SyncHttpApi._
//import com.wavesplatform.it.config.DexTestConfig._
//
///**
//  * BUY orders:  (1 - p) * best bid <= price <= (1 + l) * best ask
//  * SELL orders: (1 - l) * best bid <= price <= (1 + p) * best ask
//  *
//  * where:
//  *
//  *   p = max price deviation profit / 100
//  *   l = max price deviation loss / 100
//  *   best bid = highest price of buy
//  *   best ask = lowest price of sell
//  */
//class OrderDeviationsTestSuite extends MatcherSuiteBase {
//
//  override protected def nodeConfigs: Seq[Config] = {
//    val orderDeviations =
//      s"""
//         |waves.dex {
//         |  max-price-deviations {
//         |    enable = yes
//         |    profit = 70
//         |    loss = 60
//         |    fee = 50
//         |  }
//         |}
//       """.stripMargin
//
//    super.nodeConfigs.map(ConfigFactory.parseString(orderDeviations).withFallback)
//  }
//
//  override protected def beforeAll(): Unit = {
//    super.beforeAll()
//    node.waitForTransaction(node.broadcastRequest(IssueBtcTx.json()).id)
//  }
//
//  "buy orders price is" - {
//    "in deviation bounds" in {
//      /*val bestAskOrderId  = node.placeOrder(alice, wavesBtcPair, SELL, 1000.waves, 500000, matcherFee).message.id
//      node.waitOrderStatus(wavesBtcPair, bestAskOrderId, expectedStatus = "Accepted")
//      node.orderBook(wavesBtcPair).asks shouldBe List(LevelResponse(1000.waves, 500000))
//
//      val bestBidOrderId =  node.placeOrder(bob, wavesBtcPair, BUY, 1000.waves, 400000, matcherFee)*/
//      pending
//    }
//
//    "out of deviation bounds" - {
//      "-- too low" in {
//        pending
//      }
//
//      "-- too high" in {
//        pending
//      }
//    }
//  }
//
//  "sell orders price is" - {
//    "in deviation bounds" in {
//      pending
//    }
//
//    "out of deviation bounds" - {
//      "-- too low" in {
//        pending
//      }
//
//      "-- too high" in {
//        pending
//      }
//    }
//  }
//
//  "orders fee is" - {
//    "in deviation bounds" in {
//      pending
//    }
//
//    "out of deviation bounds" in {
//      pending
//    }
//  }
//}
