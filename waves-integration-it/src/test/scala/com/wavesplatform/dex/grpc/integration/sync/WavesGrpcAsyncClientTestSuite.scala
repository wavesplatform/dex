package com.wavesplatform.dex.grpc.integration.sync

import com.typesafe.config.ConfigFactory
import com.wavesplatform.account.Address
import com.wavesplatform.dex.grpc.integration.clients.async.WavesBalancesClient.SpendableBalanceChanges
import com.wavesplatform.dex.grpc.integration.{DEXClient, ItTestSuiteBase}
import com.wavesplatform.transaction.Asset
import com.wavesplatform.transaction.Asset.{IssuedAsset, Waves}
import monix.execution.Ack
import monix.execution.Ack.Continue
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observer
import mouse.any._
import org.scalatest.{Assertion, BeforeAndAfterEach}

import scala.concurrent.Future

class WavesGrpcAsyncClientTestSuite extends ItTestSuiteBase with BeforeAndAfterEach {

  override protected val suiteInitialWavesNodeConfig = ConfigFactory.parseString("waves.dex.grpc.integration.host = 0.0.0.0")

  private var balanceChanges = Map.empty[Address, Map[Asset, Long]]

  private val eventsObserver: Observer[SpendableBalanceChanges] = new Observer[SpendableBalanceChanges] {
    override def onError(ex: Throwable): Unit                       = Unit
    override def onComplete(): Unit                                 = Unit
    override def onNext(elem: SpendableBalanceChanges): Future[Ack] = { balanceChanges = balanceChanges ++ elem; Continue }
  }

  private def assertBalanceChanges(expectedBalanceChanges: Map[Address, Map[Asset, Long]]): Assertion = eventually {
    balanceChanges.filterKeys(expectedBalanceChanges.keys.toSet) shouldBe expectedBalanceChanges
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    new DEXClient(wavesNode1GrpcApiTarget).wavesBalancesAsyncClient.unsafeTap { _.requestBalanceChanges() }.unsafeTap {
      _.spendableBalanceChanges.subscribe(eventsObserver)
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    balanceChanges = Map.empty[Address, Map[Asset, Long]]
  }

  "WavesBalancesApiGrpcServer should send balance changes via gRPC" in {
    val aliceInitialBalance = wavesNode1Api.balance(alice, Waves)
    val bobInitialBalance   = wavesNode1Api.balance(bob, Waves)

    val issueAssetTx = mkIssue(alice, "name", someAssetAmount, 2)
    val issuedAsset  = IssuedAsset(issueAssetTx.id.value)

    broadcastAndAwait(issueAssetTx)
    assertBalanceChanges {
      Map(
        alice.toAddress -> Map(
          Waves       -> (aliceInitialBalance - issueFee),
          issuedAsset -> someAssetAmount
        )
      )
    }

    broadcastAndAwait(mkTransfer(alice, bob, someAssetAmount, issuedAsset))
    assertBalanceChanges {
      Map(
        alice.toAddress -> Map(
          Waves       -> (aliceInitialBalance - issueFee - minFee),
          issuedAsset -> 0L
        ),
        bob.toAddress -> Map(
          Waves       -> bobInitialBalance,
          issuedAsset -> someAssetAmount
        )
      )
    }
  }
}
