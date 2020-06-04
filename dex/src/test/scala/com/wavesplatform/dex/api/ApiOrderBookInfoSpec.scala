package com.wavesplatform.dex.api

import com.wavesplatform.dex.test.matchers.DiffMatcherWithImplicits
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json

class ApiOrderBookInfoSpec extends AnyFreeSpec with Matchers with DiffMatcherWithImplicits {

  private val json =
    """{
      |  "restrictions" : {
      |    "stepAmount" : "0.00000001",
      |    "minAmount" : "0.0000002",
      |    "maxAmount" : "3000",
      |    "stepPrice" : "0.000004",
      |    "minPrice" : "0.00005",
      |    "maxPrice" : "6000"
      |  },
      |  "matchingRules" : {
      |    "tickSize" : "0.7"
      |  }
      |}""".stripMargin

  private val orderBookInfo = ApiOrderBookInfo(
    restrictions = Some(
      ApiOrderRestrictions(
        stepAmount = 0.00000001,
        minAmount = 0.0000002,
        maxAmount = 3000,
        stepPrice = 0.000004,
        minPrice = 0.00005,
        maxPrice = 6000
      )
    ),
    matchingRules = ApiMatchingRules(
      tickSize = 0.7
    )
  )

  "backward JSON compatibility" - {
    "deserialization" in {
      Json.parse(json).as[ApiOrderBookInfo] should matchTo(orderBookInfo)
    }

    "serialization" in {
      Json.prettyPrint(Json.toJson(orderBookInfo)) should matchTo(json)
    }
  }
}
