package com.wavesplatform.dex.api

import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ActorRef, Status}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.testkit.{TestActor, TestProbe}
import cats.syntax.either._
import cats.syntax.option._
import com.google.common.primitives.Longs
import com.typesafe.config.ConfigFactory
import com.wavesplatform.dex.AddressActor.Command.PlaceOrder
import com.wavesplatform.dex.AddressActor.Query.GetTradableBalance
import com.wavesplatform.dex._
import com.wavesplatform.dex.actors.OrderBookAskAdapter
import com.wavesplatform.dex.api.http.ApiMarshallers._
import com.wavesplatform.dex.api.http.{OrderBookHttpInfo, `X-Api-Key`}
import com.wavesplatform.dex.caches.RateCache
import com.wavesplatform.dex.db.{OrderDB, WithDB}
import com.wavesplatform.dex.domain.account.{AddressScheme, KeyPair}
import com.wavesplatform.dex.domain.asset.Asset.{IssuedAsset, Waves}
import com.wavesplatform.dex.domain.asset.AssetPair
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.bytes.codec.Base58
import com.wavesplatform.dex.domain.crypto
import com.wavesplatform.dex.domain.order.OrderJson._
import com.wavesplatform.dex.domain.order.OrderType
import com.wavesplatform.dex.effect._
import com.wavesplatform.dex.gen.issuedAssetIdGen
import com.wavesplatform.dex.grpc.integration.dto.BriefAssetDescription
import com.wavesplatform.dex.market.AggregatedOrderBookActor
import com.wavesplatform.dex.market.MatcherActor.{AssetInfo, GetMarkets, GetSnapshotOffsets, MarketData, SnapshotOffsetsResponse}
import com.wavesplatform.dex.market.OrderBookActor.MarketStatus
import com.wavesplatform.dex.market.OrderBookActor.MarketStatus.marketStatusReads
import com.wavesplatform.dex.model.MatcherModel.Denormalized
import com.wavesplatform.dex.model.{LimitOrder, OrderInfo, OrderStatus, _}
import com.wavesplatform.dex.settings.OrderFeeSettings.DynamicSettings
import com.wavesplatform.dex.settings.{MatcherSettings, OrderRestrictionsSettings}
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.Eventually
import play.api.libs.json.{JsString, JsValue, Json, _}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random

class MatcherApiRouteSpec extends RouteSpec("/matcher") with MatcherSpecBase with PathMockFactory with Eventually with WithDB {

  private val apiKey       = "apiKey"
  private val apiKeyHeader = RawHeader(`X-Api-Key`.headerName, apiKey)

  private val matcherKeyPair = KeyPair("matcher".getBytes("utf-8"))
  private val smartAsset     = arbitraryAssetGen.sample.get
  private val smartAssetId   = smartAsset.id

  // Will be refactored in DEX-548
  private val (orderToCancel, sender) = orderGenerator.sample.get

  private val smartAssetDesc = BriefAssetDescription(
    name = "smart asset",
    decimals = Random.nextInt(9),
    hasScript = false
  )

  private val orderRestrictions = OrderRestrictionsSettings(
    stepAmount = 0.00000001,
    minAmount = 0.00000001,
    maxAmount = 1000.0,
    stepPrice = 0.00000001,
    minPrice = 0.00000001,
    maxPrice = 2000.0,
  )

  private val priceAssetId = issuedAssetIdGen.map(ByteStr(_)).sample.get
  private val priceAsset   = IssuedAsset(priceAssetId)

  private val smartWavesPair = AssetPair(smartAsset, Waves)
  private val smartWavesAggregatedSnapshot = OrderBookAggregatedSnapshot(
    bids = Seq(
      LevelAgg(10000000000000L, 41),
      LevelAgg(2500000000000L, 40),
      LevelAgg(300000000000000L, 1),
    ),
    asks = Seq(
      LevelAgg(50000000000L, 50),
      LevelAgg(2500000000000L, 51)
    )
  )

  private val smartWavesMarketStatus = MarketStatus(
    lastTrade = Some(LastTrade(1000, 2000, OrderType.SELL)),
    bestBid = Some(LevelAgg(1111, 2222)),
    bestAsk = Some(LevelAgg(3333, 4444))
  )

  private val (okOrder, okOrderSenderPrivateKey)   = orderGenerator.sample.get
  private val (badOrder, badOrderSenderPrivateKey) = orderGenerator.sample.get

  private val amountAssetDesc = BriefAssetDescription("AmountAsset", 8, hasScript = false)
  private val priceAssetDesc  = BriefAssetDescription("PriceAsset", 8, hasScript = false)

  private val settings = MatcherSettings.valueReader
    .read(ConfigFactory.load(), "waves.dex")
    .copy(
      priceAssets = Seq(badOrder.assetPair.priceAsset, okOrder.assetPair.priceAsset, priceAsset, Waves),
      orderRestrictions = Map(smartWavesPair -> orderRestrictions)
    )

  // getMatcherPublicKey
  routePath("/") - {
    "returns a public key" in test { route =>
      Get(routePath("/")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[JsString].value shouldBe "J6ghck2hA2GNJTHGSLSeuCjKuLDGz8i83NfCMFVoWhvf"
      }
    }
  }

  // orderBookInfo
  routePath("/orderbook/{amountAsset}/{priceAsset}/info") - {
    "returns an order book information" in test { route =>
      Get(routePath(s"/orderbook/$smartAssetId/WAVES/info")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[JsValue].as[ApiOrderBookInfo] should matchTo(
          ApiOrderBookInfo(
            restrictions = Some(orderRestrictions),
            matchingRules = ApiOrderBookInfo.MatchingRuleSettings(0.1)
          )
        )
      }
    }
  }

  // getSettings
  routePath("/matcher/settings") - {
    "returns matcher's settings" in test { route =>
      Get(routePath("/settings")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[JsValue].as[ApiMatcherPublicSettings] should matchTo(
          ApiMatcherPublicSettings(
            matcherPublicKey = matcherKeyPair.publicKey,
            matcherVersion = Version.VersionString,
            priceAssets = List(badOrder.assetPair.priceAsset, okOrder.assetPair.priceAsset, priceAsset, Waves),
            orderFee = ApiMatcherPublicSettings.ApiOrderFeeSettings.Dynamic(
              baseFee = 600000,
              rates = Map(Waves -> 1.0)
            ),
            orderVersions = List[Byte](1, 2, 3),
            networkByte = AddressScheme.current.chainId.toInt
          )
        )
      }
    }
  }

  // getRates
  routePath("/settings/rates") - {
    "returns available rates for fee" in test { route =>
      Get(routePath("/settings/rates")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[JsValue].as[ApiRates] should matchTo(ApiRates(Map(Waves -> 1.0)))
      }
    }
  }

  // getCurrentOffset
  routePath("/debug/currentOffset") - {
    "returns a current offset in the queue" in test(
      { route =>
        Get(routePath("/debug/currentOffset")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsValue].as[Int] shouldBe 0
        }
      },
      apiKey
    )
  }

  // getLastOffset
  routePath("/debug/lastOffset") - {
    "returns the last offset in the queue" in test(
      { route =>
        Get(routePath("/debug/lastOffset")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsValue].as[Int] shouldBe 0
        }
      },
      apiKey
    )
  }

  // getOldestSnapshotOffset
  routePath("/debug/oldestSnapshotOffset") - {
    "returns the oldest snapshot offset among all order books" in test(
      { route =>
        Get(routePath("/debug/oldestSnapshotOffset")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsValue].as[Int] shouldBe 100
        }
      },
      apiKey
    )
  }

  // getAllSnapshotOffsets
  routePath("/debug/allSnapshotOffsets") - {
    "returns a dictionary with order books offsets" in test(
      { route =>
        Get(routePath("/debug/allSnapshotOffsets")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsValue].as[ApiSnapshotOffsets] should matchTo(
            ApiSnapshotOffsets(
              Map(
                AssetPair(Waves, priceAsset) -> 100,
                AssetPair(smartAsset, Waves) -> 120
              )
            )
          )
        }
      },
      apiKey
    )
  }

  // saveSnapshots
  routePath("/debug/saveSnapshots") - {
    "returns that all is fine" in test(
      { route =>
        Post(routePath("/debug/saveSnapshots")).withHeaders(apiKeyHeader) ~> route ~> check {
          responseAs[JsValue].as[ApiMessage] should matchTo(ApiMessage("Saving started"))
        }
      },
      apiKey
    )
  }

  // getOrderBook
  routePath("/orderbook/{amountAsset}/{priceAsset}") - {
    "returns an order book" in test(
      { route =>
        Get(routePath(s"/orderbook/$smartAssetId/WAVES")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsValue].as[OrderBookResult].copy(timestamp = 0L) should matchTo(
            OrderBookResult(
              timestamp = 0L,
              pair = smartWavesPair,
              bids = smartWavesAggregatedSnapshot.bids,
              asks = smartWavesAggregatedSnapshot.asks
            )
          )
        }
      }
    )
  }

  // marketStatus
  routePath("/orderbook/[amountAsset]/[priceAsset]/status") - {
    "returns an order book status" in test(
      { route =>
        Get(routePath(s"/orderbook/$smartAssetId/WAVES/status")) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[MarketStatus] should matchTo(smartWavesMarketStatus)
        }
      }
    )
  }

  // placeLimitOrder
  routePath("/orderbook") - {
    "returns a placed limit order" in test(
      { route =>
        Post(routePath("/orderbook"), Json.toJson(okOrder)) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsValue].as[ApiSuccessfulPlace] should matchTo(ApiSuccessfulPlace(okOrder))
        }
      }
    )

    "returns an error if the placement of limit order was rejected" in test(
      { route =>
        Post(routePath("/orderbook"), Json.toJson(badOrder)) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[JsValue].as[ApiError] should matchTo(
            ApiError(
              error = 3148040,
              message = s"The order ${badOrder.idStr()} has already been placed",
              template = "The order {{id}} has already been placed",
              params = Json.obj(
                "id" -> badOrder.idStr()
              ),
              status = "OrderRejected"
            )
          )
        }
      }
    )
  }

  // placeMarketOrder
  routePath("/orderbook/market") - {
    "returns a placed market order" in test(
      { route =>
        Post(routePath("/orderbook/market"), Json.toJson(okOrder)) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsValue].as[ApiSuccessfulPlace] should matchTo(ApiSuccessfulPlace(okOrder))
        }
      }
    )

    "returns an error if the placement of market order was rejected" in test(
      { route =>
        Post(routePath("/orderbook/market"), Json.toJson(badOrder)) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[JsValue].as[ApiError] should matchTo(
            ApiError(
              error = 3148040,
              message = s"The order ${badOrder.idStr()} has already been placed",
              template = "The order {{id}} has already been placed",
              params = Json.obj(
                "id" -> badOrder.idStr()
              ),
              status = "OrderRejected"
            )
          )
        }
      }
    )
  }

  private val historyItem: ApiOrderBookHistoryItem = ApiOrderBookHistoryItem(
    id = okOrder.id(),
    `type` = okOrder.orderType,
    orderType = AcceptedOrderType.Limit,
    amount = okOrder.amount,
    filled = 0L,
    price = okOrder.price,
    fee = okOrder.matcherFee,
    filledFee = 0L,
    feeAsset = okOrder.feeAsset,
    timestamp = okOrder.timestamp,
    status = OrderStatus.Accepted.name,
    assetPair = okOrder.assetPair,
    avgWeighedPrice = okOrder.price // TODO Its false in case of new orders! Fix in DEX-774
  )

  // getAssetPairAndPublicKeyOrderHistory
  routePath("/orderbook/{amountAsset}/{priceAsset}/publicKey/{publicKey}") - {
    "returns an order history filtered by asset pair" in test(
      { route =>
        val now       = System.currentTimeMillis()
        val signature = crypto.sign(okOrderSenderPrivateKey, okOrder.senderPublicKey ++ Longs.toByteArray(now))
        Get(routePath(s"/orderbook/$smartAssetId/WAVES/publicKey/${okOrder.senderPublicKey}"))
          .withHeaders(
            RawHeader("Timestamp", s"$now"),
            RawHeader("Signature", s"$signature")
          ) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsArray].as[List[ApiOrderBookHistoryItem]] should matchTo(List(historyItem))
        }
      }
    )
  }

  // getPublicKeyOrderHistory
  routePath("/orderbook/{publicKey}") - {
    "returns an order history" in test(
      { route =>
        val now       = System.currentTimeMillis()
        val signature = crypto.sign(okOrderSenderPrivateKey, okOrder.senderPublicKey ++ Longs.toByteArray(now))
        Get(routePath(s"/orderbook/${okOrder.senderPublicKey}"))
          .withHeaders(
            RawHeader("Timestamp", s"$now"),
            RawHeader("Signature", s"$signature")
          ) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsArray].as[List[ApiOrderBookHistoryItem]] should matchTo(List(historyItem))
        }
      }
    )
  }

  // getAllOrderHistory
  routePath("/orders/{address}") - {
    "returns an order history by api key" in test(
      { route =>
        Get(routePath(s"/orders/${okOrder.senderPublicKey.toAddress}")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsArray].as[List[ApiOrderBookHistoryItem]] should matchTo(List(historyItem))
        }
      },
      apiKey
    )
  }

  // tradableBalance
  routePath("/orderbook/{amountAsset}/{priceAsset}/tradableBalance/{address}") - {
    "returns a tradable balance" in test(
      { route =>
        Get(routePath(s"/orderbook/$smartAssetId/WAVES/tradableBalance/${okOrder.senderPublicKey.toAddress}")) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsObject].as[ApiBalance] should matchTo(
            ApiBalance(
              Map(
                smartAsset -> 100L,
                Waves      -> 100L
              )))
        }
      }
    )
  }

  // reservedBalance
  routePath("/balance/reserved/{publicKey}") - {

    val publicKey = matcherKeyPair.publicKey
    val ts        = System.currentTimeMillis()
    val signature = crypto.sign(matcherKeyPair, publicKey ++ Longs.toByteArray(ts))

    def mkGet(route: Route)(base58PublicKey: String, ts: Long, base58Signature: String): RouteTestResult =
      Get(routePath(s"/balance/reserved/$base58PublicKey")).withHeaders(
        RawHeader("Timestamp", s"$ts"),
        RawHeader("Signature", base58Signature)
      ) ~> route

    "returns a reserved balance for specified publicKey" in test { route =>
      mkGet(route)(Base58.encode(publicKey), ts, Base58.encode(signature)) ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "works with an API key too" in test(
      { route =>
        Get(routePath(s"/balance/reserved/${Base58.encode(publicKey)}")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldBe StatusCodes.OK
        }
      },
      apiKey
    )

    "returns HTTP 400 when provided a wrong base58-encoded" - {
      "signature" in test { route =>
        mkGet(route)(Base58.encode(publicKey), ts, ";;") ~> check {
          status shouldBe StatusCodes.BadRequest
          responseAs[JsObject].as[ApiError] should matchTo(
            ApiError(
              error = 1051904,
              message = "The request has an invalid signature",
              template = "The request has an invalid signature",
              status = "InvalidSignature"
            )
          )
        }
      }

      "public key" in test { route =>
        mkGet(route)(";;", ts, Base58.encode(signature)) ~> check {
          handled shouldBe false
        }
      }
    }
  }

  // orderStatus
  routePath("/orderbook/{amountAsset}/{priceAsset}/{orderId}") - {
    "returns an order status" in test(
      { route =>
        Get(routePath(s"/orderbook/$smartAssetId/WAVES/${okOrder.id()}")) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[ApiOrderStatus] should matchTo(ApiOrderStatus("Accepted"))
        }
      }
    )
  }

  // cancel
  routePath("/orderbook/{amountAsset}/{priceAsset}/cancel") - {
    "single cancel" - {
      "returns that an order was canceled" in test(
        { route =>
          val unsignedRequest =
            CancelOrderRequest(okOrderSenderPrivateKey.publicKey, Some(okOrder.id()), timestamp = None, signature = Array.emptyByteArray)
          val signedRequest = unsignedRequest.copy(signature = crypto.sign(okOrderSenderPrivateKey, unsignedRequest.toSign))

          Post(routePath(s"/orderbook/${okOrder.assetPair.amountAssetStr}/${okOrder.assetPair.priceAssetStr}/cancel"), signedRequest) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[JsObject].as[ApiSuccessfulCancel] should matchTo(ApiSuccessfulCancel(okOrder.id()))
          }
        }
      )

      "returns an error" in test(
        { route =>
          val unsignedRequest =
            CancelOrderRequest(badOrderSenderPrivateKey.publicKey, Some(badOrder.id()), timestamp = None, signature = Array.emptyByteArray)
          val signedRequest = unsignedRequest.copy(signature = crypto.sign(badOrderSenderPrivateKey, unsignedRequest.toSign))

          Post(routePath(s"/orderbook/${badOrder.assetPair.amountAssetStr}/${badOrder.assetPair.priceAssetStr}/cancel"), signedRequest) ~> route ~> check {
            status shouldEqual StatusCodes.BadRequest
            responseAs[JsObject].as[ApiError] should matchTo(ApiError(
              error = 9437193,
              message = s"The order ${badOrder.id()} not found",
              template = "The order {{id}} not found",
              status = "OrderCancelRejected",
              params = Json.obj("id" -> badOrder.id())
            ))
          }
        }
      )
    }

    "massive cancel" - {
      "returns canceled orders" in test(
        { route =>
          val unsignedRequest = CancelOrderRequest(
            sender = okOrderSenderPrivateKey.publicKey,
            orderId = None,
            timestamp = Some(System.currentTimeMillis()),
            signature = Array.emptyByteArray
          )
          val signedRequest = unsignedRequest.copy(signature = crypto.sign(okOrderSenderPrivateKey, unsignedRequest.toSign))

          Post(routePath(s"/orderbook/${okOrder.assetPair.amountAssetStr}/${okOrder.assetPair.priceAssetStr}/cancel"), signedRequest) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[JsObject].as[ApiSuccessfulBatchCancel] should matchTo(
              ApiSuccessfulBatchCancel(
                List(
                  Right(ApiSuccessfulCancel(orderId = okOrder.id())),
                  Left(ApiError(
                    error = 25601,
                    message = "Can not persist event, please retry later or contact with the administrator",
                    template = "Can not persist event, please retry later or contact with the administrator",
                    status = "OrderCancelRejected"
                  ))
                )
              ))
          }
        }
      )

      "returns an error" in test(
        { route =>
          val unsignedRequest = CancelOrderRequest(
            sender = okOrderSenderPrivateKey.publicKey,
            orderId = None,
            timestamp = Some(System.currentTimeMillis()),
            signature = Array.emptyByteArray
          )
          val signedRequest = unsignedRequest.copy(signature = crypto.sign(okOrderSenderPrivateKey, unsignedRequest.toSign))

          Post(routePath(s"/orderbook/${badOrder.assetPair.amountAssetStr}/${badOrder.assetPair.priceAssetStr}/cancel"), signedRequest) ~> route ~> check {
            status shouldEqual StatusCodes.ServiceUnavailable
            responseAs[JsObject].as[ApiError] should matchTo(ApiError(
              error = 3145733,
              message = s"The account ${badOrder.sender.toAddress} is blacklisted",
              template = "The account {{address}} is blacklisted",
              status = "BatchCancelRejected",
              params = Json.obj(
                "address" -> badOrder.sender.toAddress
              )
            ))
          }
        }
      )
    }
  }

  // cancelAll
  routePath("/orderbook/cancel") - {
    "returns canceled orders" in test(
      { route =>
        val unsignedRequest = CancelOrderRequest(
          sender = okOrderSenderPrivateKey.publicKey,
          orderId = None,
          timestamp = Some(System.currentTimeMillis()),
          signature = Array.emptyByteArray
        )
        val signedRequest = unsignedRequest.copy(signature = crypto.sign(okOrderSenderPrivateKey, unsignedRequest.toSign))

        Post(routePath("/orderbook/cancel"), signedRequest) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[JsObject].as[ApiSuccessfulBatchCancel] should matchTo(
            ApiSuccessfulBatchCancel(
              List(
                Right(ApiSuccessfulCancel(orderId = okOrder.id())),
                Left(ApiError(
                  error = 25601,
                  message = "Can not persist event, please retry later or contact with the administrator",
                  template = "Can not persist event, please retry later or contact with the administrator",
                  status = "OrderCancelRejected"
                ))
              )
            ))
        }
      }
    )
  }

  // TODO
  // orderBooks
  routePath("/orderbook") - {
    "returns all order books" in test(
      { route =>
        Get(routePath("/orderbook")) ~> route ~> check {
          val r = responseAs[JsObject]
          (r \ "matcherPublicKey").as[String] should matchTo(matcherKeyPair.publicKey.base58)

          val markets = (r \ "markets").as[JsArray]
          markets.value.size shouldBe 1
          (markets.head.as[JsObject] - "created") should matchTo(
            Json.obj(
              "amountAssetName" -> amountAssetDesc.name,
              "amountAsset"     -> okOrder.assetPair.amountAssetStr,
              "amountAssetInfo" -> Json.obj(
                "decimals" -> amountAssetDesc.decimals
              ),
              "priceAssetName" -> priceAssetDesc.name,
              "priceAsset"     -> okOrder.assetPair.priceAssetStr,
              "priceAssetInfo" -> Json.obj(
                "decimals" -> priceAssetDesc.decimals
              ),
              "matchingRules" -> Json.obj(
                "tickSize" -> "0.1"
              )
            )
          )
        }
      }
    )
  }

  // orderBookDelete
  routePath("/orderbook/{amountAsset}/{priceAsset}") - {
    "returns an empty snapshot" in test(
      { route =>
        Delete(routePath(s"/orderbook/${okOrder.assetPair.amountAssetStr}/${okOrder.assetPair.priceAssetStr}"))
          .withHeaders(apiKeyHeader) ~> route ~> check {
          // TODO
        }
      },
      apiKey
    )
  }

  // getTransactionsByOrder
  routePath("/transactions/{orderId}") - {
    "returns known transactions with this order" in test(
      { route =>
        Get(routePath(s"/transactions/${okOrder.idStr()}")) ~> route ~> check {
          // TODO
          responseAs[JsArray] shouldBe Json.arr()
        }
      }
    )
  }

  routePath("/orders/cancel/[orderId]") - {
    "single cancel with API key" - {
      "returns that an order was canceled" in test(
        { route =>
          Post(routePath(s"/orders/cancel/${okOrder.id()}")).withHeaders(apiKeyHeader) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[JsObject].as[ApiSuccessfulCancel] should matchTo(ApiSuccessfulCancel(okOrder.id()))
          }
        },
        apiKey
      )

      "returns an error" in test(
        { route =>
          Post(routePath(s"/orders/cancel/${badOrder.id()}")).withHeaders(apiKeyHeader) ~> route ~> check {
            status shouldEqual StatusCodes.BadRequest
            responseAs[JsObject].as[ApiError] should matchTo(ApiError(
              error = 9437193,
              message = s"The order ${badOrder.id()} not found",
              template = "The order {{id}} not found",
              status = "OrderCancelRejected",
              params = Json.obj("id" -> badOrder.id())
            ))
          }
        },
        apiKey
      )
    }
  }

  // TODO
  routePath("/settings/rates/{assetId}") - {

    val rateCache = RateCache.inMem

    val rate        = 0.0055
    val updatedRate = 0.0067

    "add rate" in test(
      { route =>
        Put(routePath(s"/settings/rates/$smartAssetId"), rate).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.Created
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual s"The rate $rate for the asset $smartAssetId added"
          rateCache.getAllRates(smartAsset) shouldBe rate
        }
      },
      apiKey,
      rateCache
    )

    "update rate" in test(
      { route =>
        Put(routePath(s"/settings/rates/$smartAssetId"), updatedRate).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual s"The rate for the asset $smartAssetId updated, old value = $rate, new value = $updatedRate"
          rateCache.getAllRates(smartAsset) shouldBe updatedRate
        }
      },
      apiKey,
      rateCache
    )

    "update rate incorrectly (incorrect body)" in test(
      { route =>
        Put(routePath(s"/settings/rates/$smartAssetId"), "qwe").withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual "The provided JSON is invalid. Check the documentation"
        }
      },
      apiKey,
      rateCache
    )

    "update rate incorrectly (incorrect value)" in test(
      { route =>
        Put(routePath(s"/settings/rates/$smartAssetId"), 0).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual "Asset rate should be positive"
        }
      },
      apiKey,
      rateCache
    )

    "update rate incorrectly (incorrect content type)" in test(
      { route =>
        Put(routePath(s"/settings/rates/$smartAssetId"), HttpEntity(ContentTypes.`text/plain(UTF-8)`, "5"))
          .withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual "The provided JSON is invalid. Check the documentation"
        }
      },
      apiKey,
      rateCache
    )

    "delete rate" in test(
      { route =>
        Delete(routePath(s"/settings/rates/$smartAssetId")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual s"The rate for the asset $smartAssetId deleted, old value = $updatedRate"
          rateCache.getAllRates.keySet should not contain smartAsset
        }
      },
      apiKey,
      rateCache
    )

    "changing waves rate" in test(
      { route =>
        Put(routePath("/settings/rates/WAVES"), rate).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldBe StatusCodes.BadRequest
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual "The rate for WAVES cannot be changed"
        }
      },
      apiKey
    )

    "change rates without api key" in test(
      { route =>
        Put(routePath("/settings/rates/WAVES"), rate) ~> route ~> check {
          status shouldBe StatusCodes.Forbidden
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual "Provided API key is not correct"
        }
      },
      apiKey
    )

    "change rates with wrong api key" in test(
      { route =>
        Put(routePath("/settings/rates/WAVES"), rate).withHeaders(RawHeader("X-API-KEY", "wrongApiKey")) ~> route ~> check {
          status shouldBe StatusCodes.Forbidden
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual "Provided API key is not correct"
        }
      },
      apiKey
    )

    "deleting waves rate" in test(
      { route =>
        Delete(routePath("/settings/rates/WAVES")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldBe StatusCodes.BadRequest
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual "The rate for WAVES cannot be changed"
        }
      },
      apiKey
    )

    "delete rates without api key" in test(
      { route =>
        Delete(routePath("/settings/rates/WAVES")) ~> route ~> check {
          status shouldBe StatusCodes.Forbidden
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual "Provided API key is not correct"
        }
      },
      apiKey
    )

    "delete rates with wrong api key" in test(
      { route =>
        Delete(routePath("/settings/rates/WAVES")).withHeaders(RawHeader("X-API-KEY", "wrongApiKey")) ~> route ~> check {
          status shouldBe StatusCodes.Forbidden
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual "Provided API key is not correct"
        }
      },
      apiKey
    )

    "delete rate for the asset that doesn't have rate" in test(
      { route =>
        rateCache.deleteRate(smartAsset)
        Delete(routePath(s"/settings/rates/$smartAssetId")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldBe StatusCodes.NotFound
          val message = (responseAs[JsValue] \ "message").as[JsString]
          message.value shouldEqual s"The rate for the asset $smartAssetId was not specified"
        }
      },
      apiKey,
      rateCache
    )
  }

  routePath("/orderbook") - {
    "invalid field" in test { route =>
      // amount is too long
      val orderJson =
        """{
          |  "version": 1,
          |  "id": "6XHKohY1Wh8HwFx9SAf8CYwiRYBxPpWAZkHen6Whwu3i",
          |  "sender": "3N2EPHQ8hU3sFUBGcWfaS91yLpRgdQ6R8CE",
          |  "senderPublicKey": "Frfv91pfd4HUa9PxDQhyLo2nuKKtn49yMVXKpKN4gjK4",
          |  "matcherPublicKey": "77J1rZi6iyizrjH6SR9iyiKWU99MTvujDS5LUuPPqeEr",
          |  "assetPair": {
          |    "amountAsset": "7XxvP6RtKcMYEVrKZwJcaLwek4FjGkL3hWKRA6r44Pp",
          |    "priceAsset": "BbDpaEUT1R1S5fxScefViEhPmrT7rPvWhU9eYB4masS"
          |  },
          |  "orderType": "buy",
          |  "amount": 2588809419424100000000000000,
          |  "price": 22375150522026,
          |  "timestamp": 1002536707239093185,
          |  "expiration": 1576213723344,
          |  "matcherFee": 2412058533372,
          |  "signature": "4a4JP1pKtrZ5Vts2qZ9guJXsyQJaFxhJHoskzxP7hSUtDyXesFpY66REmxeDe5hUeXXMSkPP46vJXxxDPhv7hzfm",
          |  "proofs": [
          |    "4a4JP1pKtrZ5Vts2qZ9guJXsyQJaFxhJHoskzxP7hSUtDyXesFpY66REmxeDe5hUeXXMSkPP46vJXxxDPhv7hzfm"
          |  ]
          |}""".stripMargin

      Post(routePath("/orderbook"), HttpEntity(ContentTypes.`application/json`, orderJson)) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val json = responseAs[JsValue]
        (json \ "error").as[Int] shouldBe 1048577
        (json \ "params" \ "invalidFields").as[List[String]] shouldBe List("/amount")
      }
    }

    "completely invalid JSON" in test { route =>
      val orderJson = "{ I AM THE DANGEROUS HACKER"

      Post(routePath("/orderbook"), HttpEntity(ContentTypes.`application/json`, orderJson)) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val json = responseAs[JsValue]
        (json \ "error").as[Int] shouldBe 1048577
        (json \ "message").as[String] shouldBe "The provided JSON is invalid. Check the documentation"
      }
    }
  }

  // cancelAllById
  routePath("/orders/{address}/cancel") - {
    val orderId = orderToCancel.id()

    "X-Api-Key is required" in test { route =>
      Post(
        routePath(s"/orders/${orderToCancel.sender.toAddress}/cancel"),
        HttpEntity(ContentTypes.`application/json`, Json.toJson(Set(orderId)).toString())
      ) ~> route ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    "an invalid body" in test(
      { route =>
        Post(
          routePath(s"/orders/${orderToCancel.sender.toAddress}/cancel"),
          HttpEntity(ContentTypes.`application/json`, Json.toJson(orderId).toString())
        ).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
        }
      },
      apiKey
    )

    "sunny day" in test(
      { route =>
        Post(
          routePath(s"/orders/${orderToCancel.sender.toAddress}/cancel"),
          HttpEntity(ContentTypes.`application/json`, Json.toJson(Set(orderId)).toString())
        ).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      },
      apiKey
    )
  }

  // forceCancelOrder
  routePath("/orders/cancel/{orderId}") - {
    val orderId = orderToCancel.id()

    "X-Api-Key is required" in test { route =>
      Post(routePath(s"/orders/cancel/$orderId")) ~> route ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    "X-User-Public-Key is not required" in test(
      { route =>
        Post(routePath(s"/orders/cancel/$orderId")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      },
      apiKey
    )

    "X-User-Public-Key is specified, but wrong" in test(
      { route =>
        Post(routePath(s"/orders/cancel/$orderId"))
          .withHeaders(apiKeyHeader, RawHeader("X-User-Public-Key", matcherKeyPair.publicKey.base58)) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
        }
      },
      apiKey
    )

    "sunny day" in test(
      { route =>
        Post(routePath(s"/orders/cancel/$orderId"))
          .withHeaders(apiKeyHeader, RawHeader("X-User-Public-Key", orderToCancel.senderPublicKey.base58)) ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      },
      apiKey
    )
  }

  routePath("/orders/{address}/{orderId}") - {

    val testOrder = orderToCancel
    val address   = testOrder.sender.toAddress
    val orderId   = testOrder.id()

    "X-API-Key is required" in test { route =>
      Get(routePath(s"/orders/$address/$orderId")) ~> route ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    "X-User-Public-Key is not required" in test(
      { route =>
        Get(routePath(s"/orders/$address/$orderId")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      },
      apiKey
    )

    "X-User-Public-Key is specified, but wrong" in test(
      { route =>
        Get(routePath(s"/orders/$address/$orderId"))
          .withHeaders(apiKeyHeader, RawHeader("X-User-Public-Key", matcherKeyPair.publicKey.base58)) ~> route ~> check {
          status shouldEqual StatusCodes.Forbidden
        }
      },
      apiKey
    )

    "sunny day (order exists)" in test(
      { route =>
        Get(routePath(s"/orders/$address/$orderId"))
          .withHeaders(apiKeyHeader, RawHeader("X-User-Public-Key", orderToCancel.senderPublicKey.base58)) ~> route ~> check {
          status shouldEqual StatusCodes.OK
        }
      },
      apiKey
    )
  }

  routePath("/settings") - {
    "Public key should be returned" in test(
      { route =>
        Get(routePath(s"/settings")) ~> route ~> check {
          status shouldBe StatusCodes.OK
          (responseAs[JsValue] \ "matcherPublicKey").as[JsString].value shouldBe matcherKeyPair.publicKey.toString
        }
      }
    )
  }

  routePath("/orders/{publicKey}/{orderId}") - {

    val testOrder = orderToCancel
    val publicKey = sender.publicKey
    val orderId   = testOrder.id()

    "sunny day" in test { route =>
      val ts        = System.currentTimeMillis()
      val signature = Base58.encode(crypto.sign(sender, publicKey ++ Longs.toByteArray(ts)))
      Get(routePath(s"/orders/$publicKey/$orderId"))
        .withHeaders(
          RawHeader("Timestamp", ts.toString),
          RawHeader("Signature", signature)
        ) ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "invalid signature" in test { route =>
      Get(routePath(s"/orders/$publicKey/$orderId"))
        .withHeaders(
          RawHeader("Timestamp", System.currentTimeMillis.toString),
          RawHeader("Signature", "invalidSignature")
        ) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
      }
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val orderKey = MatcherKeys.order(okOrder.id())
    db.put(orderKey.keyBytes, orderKey.encode(Some(okOrder)))
  }

  private def test[U](f: Route => U, apiKey: String = "", rateCache: RateCache = RateCache.inMem): U = {

    val addressActor = TestProbe("address")
    addressActor.setAutoPilot { (sender: ActorRef, msg: Any) =>
      val response = msg match {
        case AddressDirectory.Envelope(_, msg) =>
          msg match {
            case AddressActor.Query.GetReservedBalance => AddressActor.Reply.Balance(Map(Waves -> 350L))
            case PlaceOrder(x, _)                      => if (x.id() == okOrder.id()) AddressActor.Event.OrderAccepted(x) else error.OrderDuplicate(x.id())

            case AddressActor.Query.GetOrdersStatuses(_, _) =>
              AddressActor.Reply.OrdersStatuses(List(okOrder.id() -> OrderInfo.v3(LimitOrder(okOrder), OrderStatus.Accepted)))

            case AddressActor.Query.GetOrderStatus(orderId) =>
              if (orderId == okOrder.id()) AddressActor.Reply.GetOrderStatus(OrderStatus.Accepted)
              else Status.Failure(new RuntimeException(s"Unknown order $orderId"))

            case AddressActor.Command.CancelOrder(orderId) =>
              if (orderId == okOrder.id() || orderId == orderToCancel.id()) AddressActor.Event.OrderCanceled(orderId)
              else error.OrderNotFound(orderId)

            case x @ AddressActor.Command.CancelAllOrders(pair, _) =>
              if (pair.contains(badOrder.assetPair)) error.AddressIsBlacklisted(badOrder.sender)
              else if (pair.forall(_ == okOrder.assetPair))
                AddressActor.Event.BatchCancelCompleted(
                  Map(
                    okOrder.id()  -> Right(AddressActor.Event.OrderCanceled(okOrder.id())),
                    badOrder.id() -> Left(error.CanNotPersistEvent)
                  )
                )
              else Status.Failure(new RuntimeException(s"Can't handle $x"))

            case AddressActor.Command.CancelOrders(ids) =>
              AddressActor.Event.BatchCancelCompleted(
                ids.map { id =>
                  id -> (if (id == orderToCancel.id()) Right(AddressActor.Event.OrderCanceled(okOrder.id())) else Left(error.CanNotPersistEvent))
                }.toMap
              )

            case GetTradableBalance(xs) => AddressActor.Reply.Balance(xs.map(_ -> 100L).toMap)

            case _: AddressActor.Query.GetOrderStatusInfo =>
              val ao = LimitOrder(orderToCancel)
              AddressActor.Reply.OrdersStatusInfo(OrderInfo.v4(ao, OrderStatus.Accepted).some)

            case x => Status.Failure(new RuntimeException(s"Unknown command: $x"))
          }

        case x => Status.Failure(new RuntimeException(s"Unknown message: $x"))
      }

      sender ! response
      TestActor.KeepRunning
    }

    val matcherActor = TestProbe("matcher")
    matcherActor.setAutoPilot { (sender: ActorRef, msg: Any) =>
      msg match {
        case GetSnapshotOffsets =>
          sender ! SnapshotOffsetsResponse(
            Map(
              AssetPair(Waves, priceAsset)      -> Some(100L),
              smartWavesPair                    -> Some(120L),
              AssetPair(smartAsset, priceAsset) -> None,
            )
          )

        case GetMarkets =>
          sender ! List(
            MarketData(
              pair = okOrder.assetPair,
              amountAssetName = amountAssetDesc.name,
              priceAssetName = priceAssetDesc.name,
              created = System.currentTimeMillis(),
              amountAssetInfo = Some(AssetInfo(amountAssetDesc.decimals)),
              priceAssetInfo = Some(AssetInfo(priceAssetDesc.decimals))
            )
          )
        case _ =>
      }

      TestActor.KeepRunning
    }

    val orderBookActor = TestProbe("orderBook")

    orderBookActor.setAutoPilot { (sender: ActorRef, msg: Any) =>
      msg match {
        case request: AggregatedOrderBookActor.Query.GetHttpView =>
          val assetPairDecimals = request.format match {
            case Denormalized => Some(smartAssetDesc.decimals -> 8)
            case _            => None
          }

          val entity =
            OrderBookResult(
              0L,
              smartWavesPair,
              smartWavesAggregatedSnapshot.bids,
              smartWavesAggregatedSnapshot.asks,
              assetPairDecimals
            )

          val httpResponse =
            HttpResponse(
              entity = HttpEntity(
                ContentTypes.`application/json`,
                OrderBookResult.toJson(entity)
              )
            )

          request.client ! httpResponse

        case request: AggregatedOrderBookActor.Query.GetMarketStatus => request.client ! smartWavesMarketStatus
        case _                                                       =>
      }

      TestActor.KeepRunning
    }

    val odb = OrderDB(settings.orderDb, db)
    odb.saveOrder(orderToCancel)

    val orderBooks          = new AtomicReference(Map(smartWavesPair -> orderBookActor.ref.asRight[Unit]))
    val orderBookAskAdapter = new OrderBookAskAdapter(orderBooks, 5.seconds)

    val orderBookHttpInfo =
      new OrderBookHttpInfo(
        settings = settings.orderBookSnapshotHttpCache,
        askAdapter = orderBookAskAdapter,
        time = time,
        assetDecimals = x => if (x == smartAsset) Some(smartAssetDesc.decimals) else throw new IllegalArgumentException(s"No information about $x")
      )

    val route: Route = MatcherApiRoute(
      assetPairBuilder = new AssetPairBuilder(
        settings, {
          case `smartAsset` => liftValueAsync[BriefAssetDescription](smartAssetDesc)
          case x if x == okOrder.assetPair.amountAsset || x == badOrder.assetPair.amountAsset =>
            liftValueAsync[BriefAssetDescription](amountAssetDesc)
          case x if x == okOrder.assetPair.priceAsset || x == badOrder.assetPair.priceAsset =>
            liftValueAsync[BriefAssetDescription](priceAssetDesc)
          case x => liftErrorAsync[BriefAssetDescription](error.AssetNotFound(x))
        },
        Set.empty
      ),
      matcherPublicKey = matcherKeyPair.publicKey,
      matcher = matcherActor.ref,
      addressActor = addressActor.ref,
      storeEvent = _ => Future.failed(new NotImplementedError("Storing is not implemented")),
      orderBook = {
        case x if x == okOrder.assetPair || x == badOrder.assetPair => Some(Right(orderBookActor.ref))
        case _                                                      => None
      },
      orderBookHttpInfo = orderBookHttpInfo,
      getActualTickSize = _ => 0.1,
      orderValidator = {
        case x if x == okOrder || x == badOrder => liftValueAsync(x)
        case _                                  => liftErrorAsync(error.FeatureNotImplemented)
      },
      matcherSettings = settings,
      matcherStatus = () => Matcher.Status.Working,
      db = db,
      time = time,
      currentOffset = () => 0L,
      lastOffset = () => Future.successful(0L),
      matcherAccountFee = 300000L,
      apiKeyHash = Some(crypto secureHash apiKey),
      rateCache = rateCache,
      validatedAllowedOrderVersions = () => Future.successful { Set(1, 2, 3) },
      () => DynamicSettings.symmetric(matcherFee)
    ).route

    f(route)
  }
}
