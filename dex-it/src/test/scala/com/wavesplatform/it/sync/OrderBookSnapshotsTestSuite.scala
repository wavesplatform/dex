package com.wavesplatform.it.sync

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.dex.model.OrderStatus
import com.wavesplatform.it.{MatcherSuiteBase, orderGen}
import com.wavesplatform.transaction.assets.exchange.Order
import org.scalacheck.Gen

class OrderBookSnapshotsTestSuite extends MatcherSuiteBase {
  private val interval = 50L

  override protected val suiteInitialDexConfig: Config = ConfigFactory.parseString(
    s"""waves.dex {
      |  price-assets = ["$UsdId", "WAVES"]
      |  snapshots-interval = $interval
      |}""".stripMargin
  )

  private val assetPair1 = ethUsdPair
  private val assetPair2 = ethWavesPair

  private val ordersPack1Size = 11
  private val ordersPack1 = Gen
    .containerOfN[Vector, Order](ordersPack1Size - 1, orderGen(matcher, alice, List(assetPair1)))
    .sample
    .get :+ orderGen(matcher, alice, List(assetPair2)).sample.get

  private val ordersPack2Size = interval.toInt
  private val ordersPack2 = Gen
    .containerOfN[Vector, Order](ordersPack2Size, orderGen(matcher, alice, List(assetPair2)))
    .sample
    .get

  override protected def beforeAll(): Unit = {
    startAndWait(wavesNode1Container(), wavesNode1Api)
    broadcastAndAwait(IssueEthTx, IssueUsdTx)
    startAndWait(dex1Container(), dex1Api)
  }

  "Order books are created with right offsets" in {

    ordersPack1.foreach(dex1Api.place)
    dex1Api.waitForCurrentOffset(_ == ordersPack1Size - 1)

    val allSnapshotOffsets1 = dex1Api.allSnapshotOffsets

    withClue("We doesn't show pairs, those have snapshot's offset equal to -1") {
      if (allSnapshotOffsets1.contains(assetPair1)) allSnapshotOffsets1(assetPair1) should be < interval
      if (allSnapshotOffsets1.contains(assetPair2)) allSnapshotOffsets1(assetPair2) should be < interval
    }

    ordersPack2.foreach(dex1Api.place)
    dex1Api.waitForCurrentOffset(_ == ordersPack1Size + ordersPack2Size - 1)
    val allSnapshotOffsets2 = dex1Api.allSnapshotOffsets
    withClue("Asset pairs has right offsets") {
      allSnapshotOffsets2.foreach {
        case (pair, offset) =>
          withClue(pair) {
            offset should be < (interval * 2)
          }
      }
    }
  }

  "All events are processed after restart" in {
    restartContainer(dex1Container(), dex1Api)
    dex1Api.waitForCurrentOffset(_ == ordersPack1Size + ordersPack2Size - 1)
    ordersPack1.foreach { order =>
      dex1Api.orderStatus(order) should not be OrderStatus.NotFound.name
    }
  }
}
