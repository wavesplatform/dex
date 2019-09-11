package com.wavesplatform.it.sync.smartcontracts

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.api.http.ApiError.TransactionNotAllowedByAssetScript
import com.wavesplatform.common.utils.EitherExt2
import com.wavesplatform.features.BlockchainFeatures
import com.wavesplatform.it.NewMatcherSuiteBase
import com.wavesplatform.it.api.FeeConstants._
import com.wavesplatform.it.api.{MatcherError, OrderStatus}
import com.wavesplatform.it.blockchain.MkEntities
import com.wavesplatform.it.config.DexTestConfig._
import com.wavesplatform.lang.script.v1.ExprScript
import com.wavesplatform.lang.v1.compiler.Terms
import com.wavesplatform.transaction.Asset.{IssuedAsset, Waves}
import com.wavesplatform.transaction.assets.exchange.{AssetPair, Order, OrderType}

/**
  * Rules:
  * 1. If the script fails during placing, the matcher must reject an order
  * 2. If the script fails during execution, the matcher must cancel both orders (submitted and counter)
  */
class OrdersFromScriptedAssetTestSuite extends NewMatcherSuiteBase {

  import OrdersFromScriptedAssetTestSuite._

  override protected val suiteInitialWavesNodeConfig: Config =
    ConfigFactory
      .parseString(
        s"""waves.blockchain.custom.functionality.pre-activated-features = {
           |  ${BlockchainFeatures.SmartAssets.id} = 0,
           |  ${BlockchainFeatures.SmartAccountTrading.id} = $activationHeight
           |}""".stripMargin
      )

  override protected val suiteInitialDexConfig: Config =
    ConfigFactory.parseString(s"""waves.dex.price-assets = ["${allowAsset.id}", "${denyAsset.id}", "${unscriptedAsset.id}"]""")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    broadcastAndAwait(issueUnscriptedAssetTx, issueAllowAssetTx, issueAllowAsset2Tx, issueAllowAsset3Tx, issueDenyAssetTx)
  }

  "can match orders when SmartAccTrading is still not activated" in {
    val pair = AssetPair(Waves, allowAsset)

    val counter = mkOrder(matcher, pair, OrderType.SELL, 100000, 2 * Order.PriceConstant, version = 1, matcherFee = smartTradeFee)
    dex1Api.place(counter)
    dex1Api.waitForOrderStatus(counter, OrderStatus.Accepted)

    val submitted = mkOrder(matcher, pair, OrderType.BUY, 100000, 2 * Order.PriceConstant, version = 1, matcherFee = smartTradeFee)
    dex1Api.place(submitted)
    dex1Api.waitForOrderStatus(submitted, OrderStatus.Filled)

    waitForOrderAtNode(submitted.id())
  }

  "wait activation" in wavesNode1Api.waitForHeight(activationHeight)

  "can place if the script returns TRUE" in {
    val pair    = AssetPair(unscriptedAsset, allowAsset)
    val counter = mkOrder(matcher, pair, OrderType.SELL, 100000, 2 * Order.PriceConstant, version = 2, matcherFee = smartTradeFee)
    dex1Api.place(counter)
    dex1Api.waitForOrderStatus(counter, OrderStatus.Accepted)
    dex1Api.cancel(matcher, counter)
  }

  "can't place if the script returns FALSE" in {
    val pair  = AssetPair(unscriptedAsset, denyAsset)
    val order = mkOrder(matcher, pair, OrderType.BUY, 100000, 2 * Order.PriceConstant, matcherFee = smartTradeFee, version = 2)
    dex1Api.tryPlace(order) should failWith(11536130, MatcherError.Params(assetId = Some(denyAsset.id.toString)))
  }

  "can execute against unscripted, if the script returns TRUE" in {
    val pair = AssetPair(unscriptedAsset, allowAsset)

    info("place counter")
    val counter = mkOrder(matcher, pair, OrderType.SELL, 100000, 2 * Order.PriceConstant, version = 2, matcherFee = smartTradeFee)
    dex1Api.place(counter)
    dex1Api.waitForOrderStatus(counter, OrderStatus.Accepted)

    info("place a submitted order")
    val submitted = mkOrder(matcher, pair, OrderType.BUY, 100000, 2 * Order.PriceConstant, version = 2, matcherFee = smartTradeFee)
    dex1Api.place(submitted)
    dex1Api.waitForOrderStatus(submitted, OrderStatus.Filled)
  }

  "can execute against scripted, if both scripts returns TRUE" in {
    val allowAsset100 = mkAllow(100)
    broadcastAndAwait(allowAsset100)

    val pair = AssetPair(IssuedAsset(allowAsset100.id()), allowAsset)

    info("place a counter order")
    val counter = mkOrder(matcher, pair, OrderType.SELL, 100000, 2 * Order.PriceConstant, version = 2, matcherFee = twoSmartTradeFee)
    dex1Api.place(counter)
    dex1Api.waitForOrderStatus(counter, OrderStatus.Accepted)

    info("place a submitted order")
    val submitted = mkOrder(matcher, pair, OrderType.BUY, 100000, 2 * Order.PriceConstant, version = 2, matcherFee = twoSmartTradeFee)
    dex1Api.place(submitted)

    info("both orders are cancelled")
    dex1Api.waitForOrderStatus(submitted, OrderStatus.Filled)
    dex1Api.waitForOrderStatus(counter, OrderStatus.Filled)
  }

  "can't execute against unscripted, if the script returns FALSE" in {
    info("place a counter order")
    val pair    = AssetPair(allowAsset2, unscriptedAsset)
    val counter = mkOrder(matcher, pair, OrderType.SELL, 100001, 2 * Order.PriceConstant, version = 2, matcherFee = smartTradeFee)
    dex1Api.place(counter)
    dex1Api.waitForOrderStatus(counter, OrderStatus.Accepted)

    info("update a script")
    val setAssetScript = mkSetAssetScriptText(matcher, allowAsset2, DenyBigAmountScript)
    broadcastAndAwait(setAssetScript)

    info("a counter order wasn't rejected")
    dex1Api.orderStatus(counter).status shouldBe OrderStatus.Accepted

    info("place a submitted order")
    val submitted = mkOrder(matcher, pair, OrderType.BUY, 100000, 2 * Order.PriceConstant, version = 2, matcherFee = smartTradeFee)
    dex1Api.place(submitted)

    info("two orders form an invalid transaction")
    dex1Api.waitForOrderStatus(submitted, OrderStatus.Filled)
    dex1Api.waitForOrderStatus(counter, OrderStatus.PartiallyFilled)

    val txs = dex1Api.waitForTransactionsByOrder(submitted, 1)
    val r   = wavesNode1Api.tryBroadcast(txs.head)
    r shouldBe 'left
    r.left.get.error shouldBe TransactionNotAllowedByAssetScript.ErrorCode
  }

  "can't execute against scripted, if one script returns FALSE" in {
    info("place a counter order")
    val pair    = AssetPair(allowAsset3, allowAsset)
    val counter = mkOrder(matcher, pair, OrderType.SELL, 100001, 2 * Order.PriceConstant, version = 2, matcherFee = twoSmartTradeFee)
    dex1Api.place(counter)
    dex1Api.waitForOrderStatus(counter, OrderStatus.Accepted)

    info("update a script")
    val setAssetScriptTx = mkSetAssetScriptText(matcher, allowAsset3, DenyBigAmountScript)
    broadcastAndAwait(setAssetScriptTx)

    info("a counter order wasn't rejected")
    dex1Api.orderStatus(counter).status shouldBe OrderStatus.Accepted

    info("place a submitted order")
    val submitted = mkOrder(matcher, pair, OrderType.BUY, 100000, 2 * Order.PriceConstant, version = 2, matcherFee = twoSmartTradeFee)
    dex1Api.place(submitted)

    info("two orders form an invalid transaction")
    dex1Api.waitForOrderStatus(submitted, OrderStatus.Filled)
    dex1Api.waitForOrderStatus(counter, OrderStatus.PartiallyFilled)

    val txs = dex1Api.waitForTransactionsByOrder(submitted, 1)
    val r   = wavesNode1Api.tryBroadcast(txs.head)
    r shouldBe 'left
    r.left.get.error shouldBe TransactionNotAllowedByAssetScript.ErrorCode
  }
}

object OrdersFromScriptedAssetTestSuite {

  import MkEntities.mk
  private def mkAllow(id: Int) = mk(matcher, s"AllowAsset-$id", Int.MaxValue / 3, 0, smartIssueFee, Some(ExprScript(Terms.TRUE).explicitGet()))

  private val issueUnscriptedAssetTx = mk(matcher, "UnscriptedAsset", Int.MaxValue / 3, 0)
  private val unscriptedAsset        = IssuedAsset(issueUnscriptedAssetTx.id())

  private val issueAllowAssetTx = mkAllow(0)
  private val allowAsset        = IssuedAsset(issueAllowAssetTx.id())

  private val issueAllowAsset2Tx = mkAllow(1)
  private val allowAsset2        = IssuedAsset(issueAllowAsset2Tx.id())

  private val issueAllowAsset3Tx = mkAllow(2)
  private val allowAsset3        = IssuedAsset(issueAllowAsset3Tx.id())

  private val issueDenyAssetTx = mk(matcher, "DenyAsset", Int.MaxValue / 3, 0, smartIssueFee, Some(ExprScript(Terms.FALSE).explicitGet()))
  private val denyAsset        = IssuedAsset(issueDenyAssetTx.id())

  private val DenyBigAmountScript: String =
    s"""{-# STDLIB_VERSION 2 #-}
       |match tx {
       | case tx: ExchangeTransaction => tx.sellOrder.amount <= 100000
       | case other => true
       |}""".stripMargin

  private val activationHeight = 5
}
