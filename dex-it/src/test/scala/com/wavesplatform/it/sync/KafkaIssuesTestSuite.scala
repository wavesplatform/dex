package com.wavesplatform.it.sync

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.dex.api.websockets.{WsBalances, WsOrder}
import com.wavesplatform.dex.domain.asset.Asset
import com.wavesplatform.dex.domain.asset.Asset.Waves
import com.wavesplatform.dex.domain.model.Denormalization._
import com.wavesplatform.dex.domain.order.OrderType.SELL
import com.wavesplatform.dex.it.api.HasKafka
import com.wavesplatform.dex.it.api.responses.dex.{OrderStatus => ApiOrderStatus}
import com.wavesplatform.dex.it.api.websockets.HasWebSockets
import com.wavesplatform.dex.model.{LimitOrder, OrderStatus}
import com.wavesplatform.it.WsSuiteBase

import scala.concurrent.duration.DurationInt

class KafkaIssuesTestSuite extends WsSuiteBase with HasWebSockets with HasKafka {

  private val requestTimeout = 15.seconds
  private val maxFailures    = 5

  // Hacks, see DEX-794
  private val deliveryTimeout         = requestTimeout + 1.second
  private val waitAfterNetworkChanges = deliveryTimeout

  override protected val dexInitialSuiteConfig: Config = ConfigFactory.parseString(s"""waves.dex { 
  price-assets = [ "$UsdId", "WAVES" ]
  events-queue {
    kafka.producer.client {
      acks = 1
      request.timeout.ms = ${requestTimeout.toMillis}
      delivery.timeout.ms = ${deliveryTimeout.toMillis}
      connections.max.idle.ms = ${deliveryTimeout.toMillis}
    }

    circuit-breaker {
      max-failures = $maxFailures
      reset-timeout = 2000ms
    }
  }
}""")

  override protected lazy val dexRunConfig = dexKafkaConfig().withFallback(jwtPublicKeyConfig)

  override protected def beforeAll(): Unit = {
    wavesNode1.start()
    kafka.start()

    broadcastAndAwait(IssueUsdTx)
    dex1.start()
  }

  "Matcher should able to restore the work after kafka issues solved" in {
    placeAndAwaitAtDex(mkOrderDP(alice, wavesUsdPair, SELL, 1.waves, 3.0))

    disconnectKafkaFromNetwork()
    Thread.sleep(waitAfterNetworkChanges.toMillis)

    val offsetBefore = dex1.api.currentOffset

    (1 to maxFailures).foreach { i =>
      dex1.api.tryPlace(mkOrderDP(alice, wavesUsdPair, SELL, i.waves, 3.0)) shouldBe 'left
    }

    Thread.sleep(requestTimeout.toMillis)
    connectKafkaToNetwork()
    Thread.sleep(waitAfterNetworkChanges.toMillis)

    withClue("Messages weren't saved to the queue") {
      dex1.api.currentOffset shouldBe offsetBefore
    }

     dex1.api.tryPlace(mkOrderDP(alice, wavesUsdPair, SELL, 1.waves, 3.0)) shouldBe 'right

    dex1.api.cancelAll(alice)
  }

  "Matcher should free reserved balances if order wasn't placed into the queue" in {

    val initialWavesBalance: Double = denormalizeWavesAmount(wavesNode1.api.balance(alice, Waves)).toDouble
    val initialUsdBalance: Double   = denormalizeAmountAndFee(wavesNode1.api.balance(alice, usd), 2).toDouble

    val wsac = mkWsAddressConnection(alice, dex1)

    assertChanges(wsac, squash = false) { Map(Waves -> WsBalances(initialWavesBalance, 0), usd -> WsBalances(initialUsdBalance, 0)) }()

    val sellOrder = mkOrderDP(alice, wavesUsdPair, SELL, 10.waves, 3.0)
    placeAndAwaitAtDex(sellOrder)

    dex1.api.reservedBalance(alice) should matchTo(Map[Asset, Long](Waves -> 10.003.waves))

    assertChanges(wsac) { Map(Waves -> WsBalances(initialWavesBalance - 10.003, 10.003)) } {
      WsOrder.fromDomain(LimitOrder(sellOrder))
    }

    disconnectKafkaFromNetwork()
    Thread.sleep(waitAfterNetworkChanges.toMillis)

    dex1.api.tryCancel(alice, sellOrder) shouldBe 'left

    val bigSellOrder = mkOrderDP(alice, wavesUsdPair, SELL, 30.waves, 3.0)
    dex1.api.tryPlace(bigSellOrder) shouldBe 'left

    dex1.api.reservedBalance(alice) should matchTo(Map[Asset, Long](Waves -> 10.003.waves))

    assertChanges(wsac, squash = false)(
      Map(Waves -> WsBalances(initialWavesBalance - 40.006, 40.006)),
      Map(Waves -> WsBalances(initialWavesBalance - 10.003, 10.003)),
    )()

    val oh = dex1.api.orderHistory(alice, Some(true))
    oh should have size 1
    oh.head.id shouldBe sellOrder.id()

    connectKafkaToNetwork()
    Thread.sleep(waitAfterNetworkChanges.toMillis)

    dex1.api.tryCancel(alice, sellOrder) shouldBe 'right
    dex1.api.waitForOrderStatus(sellOrder, ApiOrderStatus.Cancelled)

    dex1.api.orderHistory(alice, Some(true)) should have size 0
    dex1.api.reservedBalance(alice) shouldBe empty

    assertChanges(wsac, squash = false) { Map(Waves -> WsBalances(initialWavesBalance, 0)) } {
      WsOrder(id = sellOrder.id(), status = OrderStatus.Cancelled.name)
    }

    dex1.api.tryPlace(bigSellOrder) shouldBe 'right
    dex1.api.waitForOrderStatus(bigSellOrder, ApiOrderStatus.Accepted)

    dex1.api.orderHistory(alice, Some(true)) should have size 1
    dex1.api.reservedBalance(alice) should matchTo(Map[Asset, Long](Waves -> 30.003.waves))

    assertChanges(wsac, squash = false) { Map(Waves -> WsBalances(initialWavesBalance - 30.003, 30.003)) } {
      WsOrder.fromDomain(LimitOrder(bigSellOrder))
    }

    dex1.api.cancelAll(alice)
    wsac.close()
  }
}
