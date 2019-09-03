package com.wavesplatform.dex.grpc.integration.sync

import java.nio.charset.StandardCharsets

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.account.{Address, AddressScheme, KeyPair}
import com.wavesplatform.common.utils.EitherExt2
import com.wavesplatform.dex.grpc.integration.clients.BalancesServiceClient.SpendableBalanceChanges
import com.wavesplatform.dex.grpc.integration.sync.DEXExtensionTestSuite._
import com.wavesplatform.dex.grpc.integration.{DEXClient, ItTestSuiteBase}
import com.wavesplatform.it.Docker
import com.wavesplatform.it.api.SyncHttpApi._
import com.wavesplatform.it.sync.{issueFee, minFee, someAssetAmount}
import com.wavesplatform.transaction.Asset
import com.wavesplatform.transaction.Asset.{IssuedAsset, Waves}
import com.wavesplatform.transaction.assets.IssueTransactionV2
import com.wavesplatform.transaction.transfer.TransferTransactionV2
import com.wavesplatform.wallet.Wallet
import monix.execution.Ack
import monix.execution.Ack.Continue
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observer
import mouse.any._
import org.scalatest.concurrent.Eventually
import org.scalatest.{Assertion, BeforeAndAfterEach}

import scala.collection.JavaConverters._
import scala.concurrent.Future

class DEXExtensionTestSuite extends ItTestSuiteBase with BeforeAndAfterEach with Eventually {

  override protected def nodeConfigs: Seq[Config] =
    super.nodeConfigs.map(ConfigFactory.parseString("waves.dex.grpc.integration.host = 0.0.0.0").withFallback)

  private var balanceChanges = Map.empty[Address, Map[Asset, Long]]

  private val target    = s"localhost:${nodes.head.nodeExternalPort(6887)}"
  private val dexClient = new DEXClient(target)

  private val eventsObserver: Observer[SpendableBalanceChanges] = new Observer[SpendableBalanceChanges] {
    override def onError(ex: Throwable): Unit                       = Unit
    override def onComplete(): Unit                                 = Unit
    override def onNext(elem: SpendableBalanceChanges): Future[Ack] = { balanceChanges = balanceChanges ++ elem; Continue }
  }

  protected override def createDocker: Docker = new Docker(
    imageName = "com.wavesplatform/waves-integration-it:latest",
    tag = getClass.getSimpleName
  )

  private def assertBalanceChanges(expectedBalanceChanges: Map[Address, Map[Asset, Long]]): Assertion = eventually {
    balanceChanges.filterKeys(expectedBalanceChanges.keys.toSet) shouldBe expectedBalanceChanges
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    dexClient.balancesServiceClient <| { _.requestBalanceChanges() } <| { _.spendableBalanceChanges.subscribe(eventsObserver) }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    balanceChanges = Map.empty[Address, Map[Asset, Long]]
  }

  "DEX gRPC extension for the Waves node should send balance changes via gRPC" in {
    val aliceInitialBalance = node.balanceDetails(alice.toAddress.stringRepr).available
    val bobInitialBalance   = node.balanceDetails(bob.toAddress.stringRepr).available

    val issueAssetTx = IssueTransactionV2
      .selfSigned(
        chainId = AddressScheme.current.chainId,
        sender = alice,
        name = "name".getBytes(StandardCharsets.UTF_8),
        description = "description".getBytes(StandardCharsets.UTF_8),
        quantity = someAssetAmount,
        decimals = 2,
        reissuable = false,
        script = None,
        fee = issueFee,
        timestamp = System.currentTimeMillis()
      )
      .explicitGet()
    val issuedAsset = IssuedAsset(issueAssetTx.id())

    node.broadcastRequest(issueAssetTx.json()).id
    nodes.waitForTransaction(issueAssetTx.id().toString)

    assertBalanceChanges {
      Map(
        alice.toAddress -> Map(
          Waves       -> (aliceInitialBalance - issueFee),
          issuedAsset -> someAssetAmount
        )
      )
    }

    val transferTx = TransferTransactionV2
      .selfSigned(
        assetId = issuedAsset,
        sender = alice,
        recipient = bob,
        amount = someAssetAmount,
        timestamp = System.currentTimeMillis(),
        feeAssetId = Waves,
        feeAmount = minFee,
        attachment = Array.emptyByteArray
      )
      .explicitGet()

    node.broadcastRequest(transferTx.json())
    nodes.waitForTransaction(transferTx.id().toString)

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

object DEXExtensionTestSuite {
  private val accounts: Map[String, KeyPair] = {
    val config           = ConfigFactory.parseResources("genesis.conf")
    val distributionsKey = "genesis-generator.distributions"
    val distributions    = config.getObject(distributionsKey)
    distributions
      .keySet()
      .asScala
      .map { accountName =>
        val prefix   = s"$distributionsKey.$accountName"
        val seedText = config.getString(s"$prefix.seed-text")
        val nonce    = config.getInt(s"$prefix.nonce")
        accountName -> Wallet.generateNewAccount(seedText.getBytes(StandardCharsets.UTF_8), nonce)
      }
      .toMap
  }

  private val alice: KeyPair = accounts("alice")
  private val bob: KeyPair   = accounts("bob")
}
