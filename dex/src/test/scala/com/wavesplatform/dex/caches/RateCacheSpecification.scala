package com.wavesplatform.dex.caches

import com.wavesplatform.dex.db.{RateDB, WithDB}
import com.wavesplatform.dex.domain.asset.Asset
import com.wavesplatform.dex.domain.asset.Asset.Waves
import com.wavesplatform.dex.{MatcherSpecBase, NoShrink}
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.scalacheck.{ScalaCheckPropertyChecks => PropertyChecks}

class RateCacheSpecification extends AnyWordSpecLike with Matchers with WithDB with MatcherSpecBase with PropertyChecks with NoShrink {

  private def test(f: RateCache => Unit): Unit = {
    withClue("with DB")(f(RateCache(db)))
    withClue("in mem")(f(RateCache.inMem))
  }

  private val WavesRate: Map[Asset, Double] = Map(Waves -> 1d)

  "RateCache" should {

    "add, get and delete rates" in test { rc =>
      val preconditions =
        Gen
          .listOfN(
            100,
            for {
              asset <- arbitraryAssetGen
              rateValue <- Gen.choose(1, 100).map(_.toDouble / 100)
            } yield asset -> rateValue
          )
          .map(_.toMap[Asset, Double])

      forAll(preconditions) { map =>
        map.foreach { case (asset, rateValue) => rc.upsertRate(asset, rateValue) shouldBe None }
        rc.getAllRates should matchTo(map ++ WavesRate)
        map.foreach { case (asset, rate) => rc.deleteRate(asset) shouldBe Some(rate) }
        rc.getAllRates should matchTo(WavesRate)
      }
    }

    "update rate if it already exists" in test { rc =>
      forAll(arbitraryAssetGen) { asset: Asset =>
        rc.upsertRate(asset, 1) shouldBe None
        rc.getAllRates should matchTo(Map(asset -> 1d) ++ WavesRate)

        rc.upsertRate(asset, 2) shouldBe Some(1d)
        rc.getAllRates should matchTo(Map(asset -> 2d) ++ WavesRate)

        rc.deleteRate(asset) shouldBe Some(2d)
      }
    }

    "correctly restore state from db" in {
      val rateDB = RateDB(db)

      val asset1 = mkAssetId("First")
      val asset2 = mkAssetId("Second")

      rateDB.upsertRate(asset1, 1.5)
      rateDB.upsertRate(asset2, 5.1)

      RateCache(db).getAllRates should matchTo(Map(asset1 -> 1.5, asset2 -> 5.1) ++ WavesRate)
    }
  }
}
