package com.wavesplatform.it.sync.orders

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.dex.domain.asset.Asset.{IssuedAsset, Waves}
import com.wavesplatform.dex.domain.order.OrderType.{BUY, SELL}
import com.wavesplatform.dex.error.FeeNotEnough
import com.wavesplatform.dex.settings.AssetType._

class OrderPercentFeeSpendingTestSuite extends OrderFeeBaseTestSuite {

  val version = 3.toByte
  val assetType = Spending

  override protected def dexInitialSuiteConfig: Config = ConfigFactory.parseString(
    s"""
       |waves.dex {
       |  allowed-order-versions = [1, 2, 3]
       |  price-assets = [ "$UsdId", "$BtcId", "WAVES" ]
       |  order-fee.-1 {
       |    mode = percent
       |    percent {
       |      asset-type = spending
       |      min-fee = $percentFee
       |    }
       |  }
       |}""".stripMargin
  )

  override protected def beforeAll(): Unit = {
    wavesNode1.start()
    broadcastAndAwait(IssueUsdTx, IssueBtcTx)
    dex1.start()
  }

  s"V$version orders (fee asset type: $assetType) & fees processing" - {
    s"users should pay correct fee when fee asset-type = $assetType and order fully filled " in {
      val accountBuyer = mkAccountWithBalance(fullyAmountUsd + minimalFee -> IssuedAsset(UsdId))
      val accountSeller = mkAccountWithBalance(fullyAmountWaves + minimalFeeWaves -> Waves)

      placeAndAwaitAtDex(
        mkOrder(
          accountBuyer,
          wavesUsdPair,
          BUY,
          fullyAmountWaves,
          price,
          matcherFee = minimalFee,
          version = version,
          feeAsset = IssuedAsset(UsdId)
        )
      )
      placeAndAwaitAtNode(mkOrder(accountSeller, wavesUsdPair, SELL, fullyAmountWaves, price, matcherFee = minimalFeeWaves, version = version))

      wavesNode1.api.balance(accountBuyer, Waves) should be(fullyAmountWaves)
      wavesNode1.api.balance(accountBuyer, IssuedAsset(UsdId)) shouldBe 0L
      wavesNode1.api.balance(accountSeller, Waves) should be(0L)
      wavesNode1.api.balance(accountSeller, IssuedAsset(UsdId)) shouldBe fullyAmountUsd

      dex1.api.getReservedBalanceWithApiKey(accountBuyer).getOrElse(Waves, 0L) shouldBe 0L
      dex1.api.getReservedBalanceWithApiKey(accountBuyer).getOrElse(IssuedAsset(UsdId), 0L) shouldBe 0L
      dex1.api.getReservedBalanceWithApiKey(accountSeller).getOrElse(Waves, 0L) shouldBe 0L
      dex1.api.getReservedBalanceWithApiKey(accountSeller).getOrElse(IssuedAsset(UsdId), 0L) shouldBe 0L
    }

    s"users should pay correct fee when fee asset-type = $assetType and order partially filled" in {
      val accountBuyer = mkAccountWithBalance(fullyAmountUsd + minimalFee -> IssuedAsset(UsdId))
      val accountSeller = mkAccountWithBalance(partiallyAmountWaves + minimalFeeWaves -> Waves)

      placeAndAwaitAtDex(
        mkOrder(
          accountBuyer,
          wavesUsdPair,
          BUY,
          fullyAmountWaves,
          price,
          matcherFee = minimalFee,
          version = version,
          feeAsset = IssuedAsset(UsdId)
        )
      )
      placeAndAwaitAtNode(mkOrder(accountSeller, wavesUsdPair, SELL, partiallyAmountWaves, price, matcherFee = minimalFeeWaves, version = version))

      wavesNode1.api.balance(accountBuyer, Waves) shouldBe partiallyAmountWaves
      wavesNode1.api.balance(accountBuyer, IssuedAsset(UsdId)) shouldBe fullyAmountUsd + minimalFee - partiallyAmountUsd - partiallyFeeUsd
      wavesNode1.api.balance(accountSeller, Waves) shouldBe 0L
      wavesNode1.api.balance(accountSeller, IssuedAsset(UsdId)) shouldBe partiallyAmountUsd

      dex1.api.getReservedBalanceWithApiKey(accountBuyer).getOrElse(Waves, 0L) shouldBe 0L
      dex1.api
        .getReservedBalanceWithApiKey(accountBuyer)
        .getOrElse(IssuedAsset(UsdId), 0L) shouldBe fullyAmountUsd - partiallyAmountUsd + minimalFee - partiallyFeeUsd
      dex1.api.getReservedBalanceWithApiKey(accountSeller).getOrElse(Waves, 0L) shouldBe 0L
      dex1.api.getReservedBalanceWithApiKey(accountSeller).getOrElse(IssuedAsset(UsdId), 0L) shouldBe 0L
      dex1.api.cancelAllOrdersWithSig(accountBuyer)
    }

    s"order should be processed if amount less then fee when fee asset-type = $assetType" in {
      val accountBuyer = mkAccountWithBalance(fullyAmountUsd + minimalFee -> IssuedAsset(UsdId))
      val accountSeller = mkAccountWithBalance(fullyAmountWaves + tooHighFeeWaves -> Waves)

      placeAndAwaitAtDex(
        mkOrder(
          accountBuyer,
          wavesUsdPair,
          BUY,
          fullyAmountWaves,
          price,
          matcherFee = minimalFee,
          version = version,
          feeAsset = IssuedAsset(UsdId)
        )
      )
      placeAndAwaitAtNode(mkOrder(accountSeller, wavesUsdPair, SELL, fullyAmountWaves, price, matcherFee = tooHighFeeWaves, version = version))

      wavesNode1.api.balance(accountBuyer, Waves) should be(fullyAmountWaves)
      wavesNode1.api.balance(accountBuyer, IssuedAsset(UsdId)) shouldBe 0L
      wavesNode1.api.balance(accountSeller, Waves) should be(0L)
      wavesNode1.api.balance(accountSeller, IssuedAsset(UsdId)) shouldBe fullyAmountUsd

      dex1.api.getReservedBalanceWithApiKey(accountBuyer).getOrElse(Waves, 0L) shouldBe 0L
      dex1.api.getReservedBalanceWithApiKey(accountBuyer).getOrElse(IssuedAsset(UsdId), 0L) shouldBe 0L
      dex1.api.getReservedBalanceWithApiKey(accountSeller).getOrElse(Waves, 0L) shouldBe 0L
      dex1.api.getReservedBalanceWithApiKey(accountSeller).getOrElse(IssuedAsset(UsdId), 0L) shouldBe 0L
    }

    s"buy order should be rejected if fee less then minimum possible fee when fee asset-type = $assetType" in {
      dex1.tryApi
        .place(
          mkOrder(
            mkAccountWithBalance(fullyAmountUsd + minimalFee -> IssuedAsset(UsdId)),
            wavesUsdPair,
            BUY,
            fullyAmountWaves,
            price,
            tooLowFee,
            version = version,
            feeAsset = IssuedAsset(UsdId)
          )
        ) should failWith(
        FeeNotEnough.code,
        s"Required 2.52 ${UsdId.toString} as fee for this order, but given 2.51 ${UsdId.toString}"
      )
    }

    s"sell order should be rejected if fee less then minimum possible fee when fee asset-type = $assetType" in {
      dex1.tryApi
        .place(
          mkOrder(
            mkAccountWithBalance(fullyAmountWaves + tooHighFeeWaves -> Waves),
            wavesUsdPair,
            SELL,
            fullyAmountWaves,
            price,
            tooLowFeeWaves,
            version = version
          )
        ) should failWith(
        FeeNotEnough.code,
        "Required 2.1 WAVES as fee for this order, but given 2.09 WAVES"
      )
    }
  }
}
