package com.wavesplatform.dex.test

import com.wavesplatform.account.{KeyPair, PublicKey}
import com.wavesplatform.common.state.ByteStr
import com.wavesplatform.common.utils.EitherExt2
import com.wavesplatform.dex.gen._
import com.wavesplatform.transaction.Asset
import com.wavesplatform.transaction.Asset.{IssuedAsset, Waves}
import com.wavesplatform.transaction.assets.exchange._
import org.scalacheck.Gen

trait WavesEntitiesGen {
  val AssetIdLength = 32

  def issuedAssetGen: Gen[IssuedAsset] = byteArrayGen(AssetIdLength).map { xs =>
    IssuedAsset(ByteStr(xs))
  }

  val assetGen: Gen[Asset] = Gen.frequency(
    10 -> issuedAssetGen,
    1  -> Gen.const(Waves)
  )

  val orderSideGen: Gen[OrderType]      = Gen.oneOf(OrderType.BUY, OrderType.SELL)
  val orderSenderGen: Gen[KeyPair]      = bytes32gen.map(xs => KeyPair(ByteStr(xs)))
  val orderMatcherGen: Gen[PublicKey]   = orderSenderGen.map(x => x)
  val orderAssetPairGen: Gen[AssetPair] = Gen.zip(assetGen, assetGen).map(Function.tupled(AssetPair.apply))
  val orderAmountGen: Gen[Long]         = Gen.choose(1, 1000L)
  val orderPriceGen: Gen[Long]          = Gen.choose(1, 1000L).map(_ * Order.PriceConstant)
  val orderTimestampGen: Gen[Long]      = Gen.choose(1, Long.MaxValue - Order.MaxLiveTime)
  val orderTtlGen: Gen[Long]            = Gen.choose(1, Order.MaxLiveTime)
  val orderFeeGen: Gen[Long]            = Gen.choose(1, Order.MaxAmount)
  val orderFeeAssetGen: Gen[Asset]      = assetGen
  val orderVersionGen: Gen[Byte]        = Gen.oneOf(1: Byte, 2: Byte, 3: Byte)

  def orderAndSenderGen(sideGen: Gen[OrderType] = orderSideGen,
                        senderGen: Gen[KeyPair] = orderSenderGen,
                        matcherGen: Gen[PublicKey] = orderMatcherGen,
                        assetPairGen: Gen[AssetPair] = orderAssetPairGen,
                        amountGen: Gen[Long] = orderAmountGen,
                        priceGen: Gen[Long] = orderPriceGen,
                        timestampGen: Gen[Long] = orderTimestampGen,
                        ttlGen: Gen[Long] = orderTtlGen,
                        feeGen: Gen[Long] = orderFeeGen,
                        feeAssetGen: Gen[Asset] = orderFeeAssetGen,
                        versionGen: Gen[Byte] = orderVersionGen): Gen[(Order, KeyPair)] =
    for {
      tpe       <- sideGen
      sender    <- senderGen
      matcher   <- matcherGen
      assetPair <- assetPairGen
      amount    <- amountGen
      price     <- priceGen
      timestamp <- timestampGen
      ttl       <- ttlGen
      fee       <- feeGen
      feeAsset  <- feeAssetGen
      version   <- versionGen
    } yield {
      val expiration = timestamp + ttl
      val order =
        if (tpe == OrderType.BUY) Order.buy(sender, matcher, assetPair, amount, price, timestamp, expiration, fee, version, feeAsset)
        else Order.sell(sender, matcher, assetPair, amount, price, timestamp, expiration, fee, version, feeAsset)
      (order, sender)
    }

  val exchangeTransactionVersionGen: Gen[Byte]    = Gen.oneOf(1: Byte, 2: Byte)
  val exchangeTransactionFeeGen: Gen[Long]        = orderFeeGen
  val exchangeTransactionMatcherGen: Gen[KeyPair] = orderSenderGen
  val exchangeTransactionTimestampGen: Gen[Long]  = orderTimestampGen

  def exchangeTransactionGen(versionGen: Gen[Byte] = exchangeTransactionVersionGen,
                             matcherGen: Gen[KeyPair] = exchangeTransactionMatcherGen,
                             feeGen: Gen[Long] = exchangeTransactionFeeGen,
                             timestampGen: Gen[Long] = exchangeTransactionTimestampGen): Gen[ExchangeTransaction] = {
    for {
      version <- versionGen
      matcher <- matcherGen
      matcherPublicKeyGen = Gen.const[PublicKey](matcher)
      timestamp <- timestampGen
      orderTimestampGen = Gen.choose(1, 1000L).map(_ + timestamp)
      (buyOrder, _) <- {
        val sideGen = Gen.const(OrderType.BUY)
        if (version == 1)
          orderAndSenderGen(sideGen = sideGen, matcherGen = matcherPublicKeyGen, versionGen = Gen.const(1: Byte), timestampGen = orderTimestampGen)
        else orderAndSenderGen(sideGen = sideGen, matcherGen = matcherPublicKeyGen, timestampGen = orderTimestampGen)
      }
      (sellOrder, _) <- {
        val sideGen      = Gen.const(OrderType.SELL)
        val priceGen     = Gen.choose(1, buyOrder.price)
        val assetPairGen = Gen.const(buyOrder.assetPair)
        if (version == 1)
          orderAndSenderGen(sideGen = sideGen,
                            matcherGen = matcherPublicKeyGen,
                            assetPairGen = assetPairGen,
                            priceGen = priceGen,
                            versionGen = Gen.const(1: Byte),
                            timestampGen = orderTimestampGen)
        else
          orderAndSenderGen(sideGen = sideGen,
                            matcherGen = matcherPublicKeyGen,
                            assetPairGen = assetPairGen,
                            priceGen = priceGen,
                            timestampGen = orderTimestampGen)
      }
      fee       <- feeGen
    } yield {
      val amount = math.min(buyOrder.amount, sellOrder.amount)
      val price  = buyOrder.price
      val r = version match {
        case 1 =>
          ExchangeTransactionV1.create(matcher,
                                       buyOrder.asInstanceOf[OrderV1],
                                       sellOrder.asInstanceOf[OrderV1],
                                       amount,
                                       price,
                                       buyOrder.matcherFee,
                                       sellOrder.matcherFee,
                                       fee,
                                       timestamp)
        case 2 => ExchangeTransactionV2.create(matcher, buyOrder, sellOrder, amount, price, buyOrder.matcherFee, sellOrder.matcherFee, fee, timestamp)
        case x => throw new RuntimeException(s"Impossible version: $x")
      }
      r.explicitGet()
    }
  }
}
