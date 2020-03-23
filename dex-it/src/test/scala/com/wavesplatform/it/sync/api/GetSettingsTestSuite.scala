package com.wavesplatform.it.sync.api

import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.it.MatcherSuiteBase

class GetSettingsTestSuite extends MatcherSuiteBase {

  override protected def dexInitialSuiteConfig: Config = ConfigFactory.parseString(s"""waves.dex.price-assets = [ "$UsdId", "$BtcId", "WAVES" ]""")

  override protected def beforeAll(): Unit = {git
    wavesNode1.start()
    broadcastAndAwait(IssueUsdTx, IssueBtcTx)
    dex1.start()
  }

  "GET /matcher/settings should " - {
    "return correct byte of the node's network" in {
      dex1.api.settings.networkByte shouldBe 'Y'.toByte
    }

    "return correct list of price assets" in {
      dex1.api.settings.priceAssets should contain (UsdId.toString)
      dex1.api.settings.priceAssets should contain (BtcId.toString)
      dex1.api.settings.priceAssets should contain ("WAVES")
    }
  }
}
