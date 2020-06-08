package com.wavesplatform.dex.api

import com.wavesplatform.dex.domain.asset.Asset.{IssuedAsset, Waves}
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.error.ErrorFormatterContext
import com.wavesplatform.dex.model.{LevelAgg, OrderBookResult}
import com.wavesplatform.dex.test.matchers.DiffMatcherWithImplicits
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

class ApiV1OrderBookSpec extends AnyFreeSpec with Matchers with DiffMatcherWithImplicits {

  private val json = """{
                       |  "timestamp" : 0,
                       |  "bids" : [ [ "1.18", "43800.00000000" ], [ "1.17", "52187.00000000" ], [ "1.16", "809.00000000" ] ],
                       |  "asks" : [ [ "1.19", "2134.00000000" ], [ "1.20", "747.00000000" ] ]
                       |}""".stripMargin

  private val usd: Asset   = IssuedAsset("USDN".getBytes)
  private val wavesUsdPair = AssetPair(Waves, usd)

  private implicit val efc: ErrorFormatterContext = {
    case `usd` => 2
    case _     => 8
  }

  private val bids = List(LevelAgg(4380000000000L, 118), LevelAgg(5218700000000L, 117), LevelAgg(80900000000L, 116))
  private val asks = List(LevelAgg(213400000000L, 119), LevelAgg(74700000000L, 120))

  private val orderBookV1 =
    ApiV1OrderBook(
      timestamp = 0,
      bids = bids.map(la => ApiV1LevelAgg.fromLevelAgg(la, wavesUsdPair)),
      asks = asks.map(la => ApiV1LevelAgg.fromLevelAgg(la, wavesUsdPair))
    )

  private val orderBookResult = OrderBookResult(0, wavesUsdPair, bids, asks, Some(8 -> 2))

  "backward JSON compatibility" - {
    "deserialization" in {
      Json.parse(json).as[ApiV1OrderBook] should matchTo(orderBookV1)
    }

    "serialization" in {
      Json.prettyPrint(Json.parse(OrderBookResult toJson orderBookResult)) should matchTo(json)
    }
  }
}
