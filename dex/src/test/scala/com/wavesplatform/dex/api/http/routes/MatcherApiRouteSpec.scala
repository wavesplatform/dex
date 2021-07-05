package com.wavesplatform.dex.api.http.routes

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.{ActorRef, Status}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.testkit.{TestActor, TestProbe}
import cats.instances.future._
import cats.syntax.either._
import cats.syntax.option._
import com.google.common.primitives.Longs
import com.softwaremill.diffx.{Derived, Diff}
import com.typesafe.config.ConfigFactory
import com.wavesplatform.dex._
import com.wavesplatform.dex.actors.OrderBookDirectoryActor._
import com.wavesplatform.dex.actors.OrderBookAskAdapter
import com.wavesplatform.dex.actors.address.AddressActor.Command.{PlaceOrder, Source}
import com.wavesplatform.dex.actors.address.AddressActor.Query.GetTradableBalance
import com.wavesplatform.dex.actors.address.{AddressActor, AddressDirectoryActor}
import com.wavesplatform.dex.actors.orderbook.AggregatedOrderBookActor
import com.wavesplatform.dex.actors.orderbook.AggregatedOrderBookActor.MarketStatus
import com.wavesplatform.dex.api.RouteSpec
import com.wavesplatform.dex.api.http.ApiMarshallers._
import com.wavesplatform.dex.api.http.entities._
import com.wavesplatform.dex.api.http.headers.{`X-Api-Key`, CustomContentTypes}
import com.wavesplatform.dex.api.http.protocol.HttpCancelOrder
import com.wavesplatform.dex.api.http.routes.v0._
import com.wavesplatform.dex.api.http.{entities, OrderBookHttpInfo}
import com.wavesplatform.dex.api.ws.actors.WsExternalClientDirectoryActor
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.caches.RateCache
import com.wavesplatform.dex.db._
import com.wavesplatform.dex.domain.account.{Address, AddressScheme, KeyPair, PublicKey}
import com.wavesplatform.dex.domain.asset.Asset.{IssuedAsset, Waves}
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.bytes.codec.Base58
import com.wavesplatform.dex.domain.crypto
import com.wavesplatform.dex.domain.order.OrderJson._
import com.wavesplatform.dex.domain.order.OrderOps._
import com.wavesplatform.dex.domain.order.OrderType.{BUY, SELL}
import com.wavesplatform.dex.domain.order.{Order, OrderType}
import com.wavesplatform.dex.domain.transaction.ExchangeTransactionV2
import com.wavesplatform.dex.domain.utils.EitherExt2
import com.wavesplatform.dex.effect._
import com.wavesplatform.dex.error.{AddressIsBlacklisted, CanNotPersistEvent, InvalidAddress, InvalidJson, OrderDuplicate, OrderNotFound, RequestInvalidSignature, UnsupportedContentType, UserPublicKeyIsNotValid}
import com.wavesplatform.dex.gen.issuedAssetIdGen
import com.wavesplatform.dex.grpc.integration.clients.combined.CombinedStream
import com.wavesplatform.dex.grpc.integration.dto.BriefAssetDescription
import com.wavesplatform.dex.model.MatcherModel.Denormalized
import com.wavesplatform.dex.model.{LimitOrder, OrderInfo, OrderStatus, _}
import com.wavesplatform.dex.queue.{ValidatedCommand, ValidatedCommandWithMeta}
import com.wavesplatform.dex.settings.OrderFeeSettings.DynamicSettings
import com.wavesplatform.dex.settings.{MatcherSettings, OrderRestrictionsSettings}
import org.scalamock.scalatest.PathMockFactory
import org.scalatest.concurrent.Eventually
import play.api.libs.json.{JsArray, JsString, Json, JsonFacade => _}
import pureconfig.ConfigSource

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Random

class MatcherApiRouteSpec extends RouteSpec("/matcher") with MatcherSpecBase with PathMockFactory with Eventually with WithDb {

  private val apiKey = "apiKey"
  private val apiKeyHeader = RawHeader(`X-Api-Key`.headerName, apiKey)

  private val matcherKeyPair = KeyPair("matcher".getBytes("utf-8"))

  private val smartAsset = arbitraryIssuedAssetGen.sample.get
  private val smartAssetId = smartAsset.id

  private val unknownAsset = arbitraryIssuedAssetGen.sample.get
  private val unknownAssetId = unknownAsset.id

  // Will be refactored in DEX-548
  private val (orderToCancel, sender) = orderGenerator.sample.get

  private val smartAssetDesc = BriefAssetDescription(
    name = "smart asset",
    decimals = Random.nextInt(9),
    hasScript = false,
    isNft = false
  )

  private val orderRestrictions = OrderRestrictionsSettings(
    stepAmount = 0.00000001,
    minAmount = 0.00000001,
    maxAmount = 1000.0,
    stepPrice = 0.00000001,
    minPrice = 0.00000001,
    maxPrice = 2000.0
  )

  private val priceAssetId = issuedAssetIdGen.map(ByteStr(_)).sample.get
  private val priceAsset = IssuedAsset(priceAssetId)

  private val smartWavesPair = AssetPair(smartAsset, Waves)

  private val smartWavesAggregatedSnapshot = OrderBookAggregatedSnapshot(
    bids = Seq(
      LevelAgg(10000000000000L, 41),
      LevelAgg(2500000000000L, 40),
      LevelAgg(300000000000000L, 1)
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

  private val (okOrder, okOrderSenderPrivateKey) = orderGenerator.sample.get
  private val (badOrder, badOrderSenderPrivateKey) = orderGenerator.sample.get

  private val amountAssetDesc = BriefAssetDescription("AmountAsset", 8, hasScript = false, isNft = false)
  private val priceAssetDesc = BriefAssetDescription("PriceAsset", 8, hasScript = false, isNft = false)

  private val settings = ConfigSource
    .fromConfig(ConfigFactory.load())
    .at("waves.dex")
    .loadOrThrow[MatcherSettings]
    .copy(
      priceAssets = Seq(badOrder.assetPair.priceAsset, okOrder.assetPair.priceAsset, priceAsset, Waves),
      orderRestrictions = Map(smartWavesPair -> orderRestrictions)
    )

  implicit private val httpMarketDataWithMetaDiff: Derived[Diff[HttpMarketDataWithMeta]] =
    Derived(Diff.gen[HttpMarketDataWithMeta].value.ignore[HttpMarketDataWithMeta, Long](_.created))

  private def mkHistoryItem(order: Order, status: String): HttpOrderBookHistoryItem =
    HttpOrderBookHistoryItem(
      id = order.id(),
      `type` = order.orderType,
      orderType = AcceptedOrderType.Limit,
      amount = order.amount,
      filled = 0L,
      price = order.price,
      fee = order.matcherFee,
      filledFee = 0L,
      feeAsset = order.feeAsset,
      timestamp = order.timestamp,
      status = status,
      assetPair = order.assetPair,
      avgWeighedPrice = 0,
      version = order.version,
      totalExecutedPriceAssets = 0
    )

  // getMatcherPKInBase58
  routePath("/") - {
    "returns a public key in base58" in test { route =>
      Get(routePath("/")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpMatcherPublicKey] should matchTo(PublicKey.fromBase58String("J6ghck2hA2GNJTHGSLSeuCjKuLDGz8i83NfCMFVoWhvf").explicitGet())
      }
    }
  }

  // getOrderBookRestrictions
  routePath("/orderbook/{amountAsset}/{priceAsset}/info") - {
    "returns an order book information" in test { route =>
      Get(routePath(s"/orderbook/$smartAssetId/WAVES/info")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpOrderBookInfo] should matchTo(
          HttpOrderBookInfo(
            restrictions = Some(HttpOrderRestrictions.fromSettings(orderRestrictions)),
            matchingRules = HttpMatchingRules(0.1)
          )
        )
      }
    }
  }

  // getMatcherPublicSettings
  routePath("/matcher/settings") - {
    "returns matcher's public settings" in test { route =>
      Get(routePath("/settings")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpMatcherPublicSettings] should matchTo(
          HttpMatcherPublicSettings(
            matcherPublicKey = matcherKeyPair.publicKey,
            matcherVersion = Version.VersionString,
            priceAssets = List(badOrder.assetPair.priceAsset, okOrder.assetPair.priceAsset, priceAsset, Waves),
            orderFee = HttpOrderFeeMode.FeeModeDynamic(
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

  // getAssetRates
  routePath("/settings/rates") - {
    "returns available asset rates for fee" in test { route =>
      Get(routePath("/settings/rates")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpRates] should matchTo(Map[Asset, Double](Waves -> 1.0))
      }
    }
  }

  // getCurrentOffset
  routePath("/debug/currentOffset") - {
    "returns a current offset in the queue" in test(
      route =>
        Get(routePath("/debug/currentOffset")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpOffset] should matchTo(0L)
        },
      apiKey
    )
  }

  // getLastOffset
  routePath("/debug/lastOffset") - {
    "returns the last offset in the queue" in test(
      route =>
        Get(routePath("/debug/lastOffset")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpOffset] should matchTo(0L)
        },
      apiKey
    )
  }

  // getMatcherConfig
  routePath("/debug/config") - {
    "X-Api-Key is required" in test { route =>
      Get(routePath("/debug/config")) ~> route ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    "returns application/hocon as content-type" in test(
      route =>
        Get(routePath("/debug/config")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual CustomContentTypes.`application/hocon`
        },
      apiKey
    )
  }

  // getOldestSnapshotOffset
  routePath("/debug/oldestSnapshotOffset") - {
    "returns the oldest snapshot offset among all order books" in test(
      route =>
        Get(routePath("/debug/oldestSnapshotOffset")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpOffset] should matchTo(100L)
        },
      apiKey
    )
  }

  // getAllSnapshotOffsets
  routePath("/debug/allSnapshotOffsets") - {
    "returns a dictionary with order books offsets" in test(
      route =>
        Get(routePath("/debug/allSnapshotOffsets")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpSnapshotOffsets] should matchTo(
            Map(
              AssetPair(Waves, priceAsset) -> 100L,
              AssetPair(smartAsset, Waves) -> 120L
            )
          )
        },
      apiKey
    )
  }

  // saveSnapshots
  routePath("/debug/saveSnapshots") - {
    "returns that all is fine" in test(
      route =>
        Post(routePath("/debug/saveSnapshots")).withHeaders(apiKeyHeader) ~> route ~> check {
          responseAs[HttpMessage] should matchTo(HttpMessage("Saving started"))
        },
      apiKey
    )
  }

  // getOrderBook
  routePath("/orderbook/{amountAsset}/{priceAsset}") - {
    "returns an order book" in test { route =>
      Get(routePath(s"/orderbook/$smartAssetId/WAVES")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpV0OrderBook] should matchTo(
          HttpV0OrderBook(
            timestamp = 0L,
            pair = smartWavesPair,
            bids = smartWavesAggregatedSnapshot.bids.toList.map(HttpV0LevelAgg.fromLevelAgg),
            asks = smartWavesAggregatedSnapshot.asks.toList.map(HttpV0LevelAgg.fromLevelAgg)
          )
        )
      }
    }
  }

  // getOrderBookStatus
  routePath("/orderbook/[amountAsset]/[priceAsset]/status") - {
    "returns an order book status" in test { route =>
      Get(routePath(s"/orderbook/$smartAssetId/WAVES/status")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpOrderBookStatus] should matchTo(HttpOrderBookStatus fromMarketStatus smartWavesMarketStatus)
      }
    }

    "returns OK even there is no such order book" in test { route =>
      Get(routePath(s"/orderbook/$unknownAssetId/WAVES/status")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpOrderBookStatus] should matchTo(HttpOrderBookStatus(None, None, None, None, None, None, None))
      }
    }
  }

  // placeLimitOrder
  routePath("/orderbook") - {

    "returns an error if Content-Type is unsupported" in test { route =>
      Post(
        routePath("/orderbook"),
        HttpEntity(ContentTypes.`text/xml(UTF-8)`, Json.toJson(okOrder).toString())
      ) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[HttpError] should matchTo(
          HttpError(
            error = UnsupportedContentType.code,
            message = "The provided Content-Type is not supported, please provide JSON",
            template = "The provided Content-Type is not supported, please provide JSON",
            status = "InvalidJsonResponse"
          )
        )
      }
    }

    "returns a placed limit order" in test { route =>
      Post(routePath("/orderbook"), Json.toJson(okOrder)) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpSuccessfulPlace] should matchTo(HttpSuccessfulPlace(okOrder))
      }
    }

    "returns an error if the placement of limit order was rejected" in test { route =>
      Post(routePath("/orderbook"), Json.toJson(badOrder)) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[HttpError] should matchTo(
          HttpError(
            error = OrderDuplicate.code,
            message = s"The order ${badOrder.idStr()} has already been placed",
            template = "The order {{id}} has already been placed",
            params = Json.obj("id" -> badOrder.idStr()),
            status = "OrderRejected"
          )
        )
      }
    }
  }

  // placeMarketOrder
  routePath("/orderbook/market") - {
    "returns a placed market order" in test { route =>
      Post(routePath("/orderbook/market"), Json.toJson(okOrder)) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpSuccessfulPlace] should matchTo(HttpSuccessfulPlace(okOrder))
      }
    }

    "returns an error if the placement of market order was rejected" in test { route =>
      Post(routePath("/orderbook/market"), Json.toJson(badOrder)) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[HttpError] should matchTo(
          HttpError(
            error = OrderDuplicate.code,
            message = s"The order ${badOrder.idStr()} has already been placed",
            template = "The order {{id}} has already been placed",
            params = Json.obj("id" -> badOrder.idStr()),
            status = "OrderRejected"
          )
        )
      }
    }
  }

  private val historyItem: HttpOrderBookHistoryItem = mkHistoryItem(okOrder, OrderStatus.Accepted.name)

  // getOrderHistoryByAssetPairAndPKWithSig
  routePath("/orderbook/{amountAsset}/{priceAsset}/publicKey/{publicKey}") - {
    "returns an order history filtered by asset pair" in test { route =>
      val now = System.currentTimeMillis()
      val signature = crypto.sign(okOrderSenderPrivateKey, okOrder.senderPublicKey ++ Longs.toByteArray(now))
      Get(routePath(s"/orderbook/$smartAssetId/WAVES/publicKey/${okOrder.senderPublicKey}"))
        .withHeaders(
          RawHeader("Timestamp", s"$now"),
          RawHeader("Signature", s"$signature")
        ) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[List[HttpOrderBookHistoryItem]] should matchTo(List(historyItem))
      }
    }
  }

  // getOrderHistoryByPKWithSig
  routePath("/orderbook/{publicKey}") - {
    "returns an order history" in test { route =>
      val now = System.currentTimeMillis()
      val signature = crypto.sign(okOrderSenderPrivateKey, okOrder.senderPublicKey ++ Longs.toByteArray(now))
      Get(routePath(s"/orderbook/${okOrder.senderPublicKey}"))
        .withHeaders(
          RawHeader("Timestamp", s"$now"),
          RawHeader("Signature", s"$signature")
        ) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[List[HttpOrderBookHistoryItem]] should matchTo(List(historyItem))
      }
    }
  }

  // getOrderHistoryByAddressWithKey
  routePath("/orders/{address}") - {
    "returns an order history by api key" in test(
      route =>
        Get(routePath(s"/orders/${okOrder.senderPublicKey.toAddress}")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[List[HttpOrderBookHistoryItem]] should matchTo(List(historyItem))
        },
      apiKey
    )
  }

  // getTradableBalanceByAssetPairAndAddress
  routePath("/orderbook/{amountAsset}/{priceAsset}/tradableBalance/{address}") - {

    "returns a tradable balance" in test { route =>
      Get(routePath(s"/orderbook/$smartAssetId/WAVES/tradableBalance/${okOrder.senderPublicKey.toAddress}")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpBalance] should matchTo(
          Map(
            smartAsset -> 100L,
            Waves -> 100L
          )
        )
      }
    }

    "returns error when address is from the other network" in test { route =>
      val otherNetworkChainId: Byte = 99
      val addressFromOtherNetwork =
        Address.fromPublicKey(mkKeyPair(s"seed-${ThreadLocalRandom.current().nextInt}").publicKey, otherNetworkChainId)

      val currentNetwork = s"${AddressScheme.current.chainId}(${AddressScheme.current.chainId.toChar})"
      val otherNetwork = s"$otherNetworkChainId(${otherNetworkChainId.toChar})"

      Get(routePath(s"/orderbook/$smartAssetId/WAVES/tradableBalance/$addressFromOtherNetwork")) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[HttpError] should matchTo(
          HttpError(
            error = InvalidAddress.code,
            message = s"Provided address in not correct, reason: Data from other network: expected: $currentNetwork, actual: $otherNetwork",
            template = "Provided address in not correct, reason: {{reason}}",
            params = Json.obj("reason" -> s"Data from other network: expected: $currentNetwork, actual: $otherNetwork"),
            status = "InvalidAddress"
          )
        )
      }
    }
  }

  // getReservedBalanceByPK
  routePath("/balance/reserved/{publicKey}") - {

    val publicKey = matcherKeyPair.publicKey
    val ts = System.currentTimeMillis()
    val signature = crypto.sign(matcherKeyPair, publicKey ++ Longs.toByteArray(ts))

    def mkGet(route: Route)(base58PublicKey: String, ts: Long, base58Signature: String): RouteTestResult =
      Get(routePath(s"/balance/reserved/$base58PublicKey")).withHeaders(
        RawHeader("Timestamp", s"$ts"),
        RawHeader("Signature", base58Signature)
      ) ~> route

    "returns a reserved balance for specified publicKey" in test { route =>
      mkGet(route)(Base58.encode(publicKey), ts, Base58.encode(signature)) ~> check {
        status shouldBe StatusCodes.OK
        responseAs[HttpBalance] should matchTo(Map[Asset, Long](Waves -> 350L))
      }
    }

    "works with an API key too" in test(
      route =>
        Get(routePath(s"/balance/reserved/${Base58.encode(publicKey)}")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldBe StatusCodes.OK
          responseAs[HttpBalance] should matchTo(Map[Asset, Long](Waves -> 350L))
        },
      apiKey
    )

    "returns HTTP 400 when provided a wrong base58-encoded" - {
      "signature" in test { route =>
        mkGet(route)(Base58.encode(publicKey), ts, ";;") ~> check {
          status shouldBe StatusCodes.BadRequest
          responseAs[HttpError] should matchTo(
            HttpError(
              error = RequestInvalidSignature.code,
              message = "The request has an invalid signature",
              template = "The request has an invalid signature",
              status = "InvalidSignature"
            )
          )
        }
      }

      "public key" in test { route =>
        mkGet(route)(";;", ts, Base58.encode(signature)) ~> check {
          responseAs[HttpError] should matchTo(
            HttpError(
              error = UserPublicKeyIsNotValid.code,
              message =
                "Provided public key is not correct, reason: Unable to decode base58: requirement failed: Wrong char ';' in Base58 string ';;'",
              template = "Provided public key is not correct, reason: {{reason}}",
              params = Some(Json.obj("reason" -> "Unable to decode base58: requirement failed: Wrong char ';' in Base58 string ';;'")),
              status = "InvalidPublicKey"
            )
          )
        }
      }
    }
  }

  // getOrderStatusByAssetPairAndId
  routePath("/orderbook/{amountAsset}/{priceAsset}/{orderId}") - {
    "returns an order status" in test { route =>
      Get(routePath(s"/orderbook/$smartAssetId/WAVES/${okOrder.id()}")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpOrderStatus] should matchTo(entities.HttpOrderStatus(HttpOrderStatus.Status.Accepted))
      }
    }
  }

  // getOrderStatusByAddressAndIdWithKey
  routePath("/orders/{address}/{orderId}") - {

    val testOrder = orderToCancel
    val address = testOrder.sender.toAddress
    val orderId = testOrder.id()

    "X-API-Key is required" in test { route =>
      Get(routePath(s"/orders/$address/$orderId")) ~> route ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    "X-User-Public-Key is not required" in test(
      route =>
        Get(routePath(s"/orders/$address/$orderId")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpOrderBookHistoryItem] should matchTo(mkHistoryItem(orderToCancel, OrderStatus.Accepted.name))
        },
      apiKey
    )

    "X-User-Public-Key is specified, but wrong" in test(
      route =>
        Get(routePath(s"/orders/$address/$orderId"))
          .withHeaders(apiKeyHeader, RawHeader("X-User-Public-Key", matcherKeyPair.publicKey.base58)) ~> route ~> check {
          status shouldEqual StatusCodes.Forbidden
        },
      apiKey
    )

    "sunny day (order exists)" in test(
      route =>
        Get(routePath(s"/orders/$address/$orderId"))
          .withHeaders(apiKeyHeader, RawHeader("X-User-Public-Key", orderToCancel.senderPublicKey.base58)) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpOrderBookHistoryItem] should matchTo(mkHistoryItem(orderToCancel, OrderStatus.Accepted.name))
        },
      apiKey
    )
  }

  // getOrderStatusByPKAndIdWithSig
  routePath("/orderbook/{publicKey}/{orderId}") - {

    val testOrder = orderToCancel
    val publicKey = sender.publicKey
    val orderId = testOrder.id()

    "sunny day" in test { route =>
      val ts = System.currentTimeMillis()
      val signature = Base58.encode(crypto.sign(sender, publicKey ++ Longs.toByteArray(ts)))
      Get(routePath(s"/orderbook/$publicKey/$orderId"))
        .withHeaders(
          RawHeader("Timestamp", ts.toString),
          RawHeader("Signature", signature)
        ) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpOrderBookHistoryItem] should matchTo(mkHistoryItem(orderToCancel, OrderStatus.Accepted.name))
      }
    }

    "invalid signature" in test { route =>
      Get(routePath(s"/orderbook/$publicKey/$orderId"))
        .withHeaders(
          RawHeader("Timestamp", System.currentTimeMillis.toString),
          RawHeader("Signature", "invalidSignature")
        ) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
      }
    }
  }

  // cancelOneOrAllInPairOrdersWithSig
  routePath("/orderbook/{amountAsset}/{priceAsset}/cancel") - {

    "single cancel" - {
      "returns that an order was canceled" in test { route =>
        val unsignedRequest =
          HttpCancelOrder(okOrderSenderPrivateKey.publicKey, Some(okOrder.id()), timestamp = None, signature = Array.emptyByteArray)
        val signedRequest = unsignedRequest.copy(signature = crypto.sign(okOrderSenderPrivateKey, unsignedRequest.toSign))

        Post(
          routePath(s"/orderbook/${okOrder.assetPair.amountAssetStr}/${okOrder.assetPair.priceAssetStr}/cancel"),
          signedRequest
        ) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpSuccessfulSingleCancel] should matchTo(HttpSuccessfulSingleCancel(okOrder.id()))
        }
      }

      "returns an error" in test { route =>
        val unsignedRequest =
          HttpCancelOrder(badOrderSenderPrivateKey.publicKey, Some(badOrder.id()), timestamp = None, signature = Array.emptyByteArray)
        val signedRequest = unsignedRequest.copy(signature = crypto.sign(badOrderSenderPrivateKey, unsignedRequest.toSign))

        Post(
          routePath(s"/orderbook/${badOrder.assetPair.amountAssetStr}/${badOrder.assetPair.priceAssetStr}/cancel"),
          signedRequest
        ) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[HttpError] should matchTo(
            HttpError(
              error = OrderNotFound.code,
              message = s"The order ${badOrder.id()} not found",
              template = "The order {{id}} not found",
              params = Json.obj("id" -> badOrder.id()),
              status = "OrderCancelRejected"
            )
          )
        }
      }
    }

    "massive cancel" - {
      "returns canceled orders" in test { route =>
        val unsignedRequest = HttpCancelOrder(
          sender = okOrderSenderPrivateKey.publicKey,
          orderId = None,
          timestamp = Some(System.currentTimeMillis),
          signature = Array.emptyByteArray
        )
        val signedRequest = unsignedRequest.copy(signature = crypto.sign(okOrderSenderPrivateKey, unsignedRequest.toSign))

        Post(
          routePath(s"/orderbook/${okOrder.assetPair.amountAssetStr}/${okOrder.assetPair.priceAssetStr}/cancel"),
          signedRequest
        ) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpSuccessfulBatchCancel] should matchTo(
            HttpSuccessfulBatchCancel(
              List(
                Right(HttpSuccessfulSingleCancel(orderId = okOrder.id())),
                Left(
                  HttpError(
                    error = CanNotPersistEvent.code,
                    message = "Can not persist command, please retry later or contact with the administrator",
                    template = "Can not persist command, please retry later or contact with the administrator",
                    status = "OrderCancelRejected"
                  )
                )
              )
            )
          )
        }
      }

      "returns an error" in test { route =>
        val unsignedRequest = HttpCancelOrder(
          sender = okOrderSenderPrivateKey.publicKey,
          orderId = None,
          timestamp = Some(System.currentTimeMillis()),
          signature = Array.emptyByteArray
        )
        val signedRequest = unsignedRequest.copy(signature = crypto.sign(okOrderSenderPrivateKey, unsignedRequest.toSign))

        Post(
          routePath(s"/orderbook/${badOrder.assetPair.amountAssetStr}/${badOrder.assetPair.priceAssetStr}/cancel"),
          signedRequest
        ) ~> route ~> check {
          status shouldEqual StatusCodes.ServiceUnavailable
          responseAs[HttpError] should matchTo(
            HttpError(
              error = AddressIsBlacklisted.code,
              message = s"The account ${badOrder.sender.toAddress} is blacklisted",
              template = "The account {{address}} is blacklisted",
              params = Json.obj("address" -> badOrder.sender.toAddress),
              status = "BatchCancelRejected"
            )
          )
        }
      }
    }
  }

  // cancelAllOrdersWithSig
  routePath("/orderbook/cancel") - {
    "returns canceled orders" in test { route =>
      val unsignedRequest = HttpCancelOrder(
        sender = okOrderSenderPrivateKey.publicKey,
        orderId = None,
        timestamp = Some(System.currentTimeMillis),
        signature = Array.emptyByteArray
      )
      val signedRequest = unsignedRequest.copy(signature = crypto.sign(okOrderSenderPrivateKey, unsignedRequest.toSign))

      Post(routePath("/orderbook/cancel"), signedRequest) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpSuccessfulBatchCancel] should matchTo(
          HttpSuccessfulBatchCancel(
            List(
              Right(HttpSuccessfulSingleCancel(orderId = okOrder.id())),
              Left(
                HttpError(
                  error = CanNotPersistEvent.code,
                  message = "Can not persist command, please retry later or contact with the administrator",
                  template = "Can not persist command, please retry later or contact with the administrator",
                  status = "OrderCancelRejected"
                )
              )
            )
          )
        )
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
      route =>
        Post(
          routePath(s"/orders/${orderToCancel.sender.toAddress}/cancel"),
          HttpEntity(ContentTypes.`application/json`, Json.toJson(orderId).toString())
        ).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
        },
      apiKey
    )

    "sunny day" in test(
      route =>
        Post(
          routePath(s"/orders/${orderToCancel.sender.toAddress}/cancel"),
          HttpEntity(ContentTypes.`application/json`, Json.toJson(Set(orderId)).toString())
        ).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpSuccessfulBatchCancel] should matchTo(
            HttpSuccessfulBatchCancel(List(Right(HttpSuccessfulSingleCancel(orderId = orderToCancel.id()))))
          )
        },
      apiKey
    )
  }

  // getOrderBooks
  routePath("/orderbook") - {
    "returns all order books" in test { route =>
      Get(routePath("/orderbook")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[HttpTradingMarkets] should matchTo(
          HttpTradingMarkets(
            matcherKeyPair.publicKey,
            Seq(
              HttpMarketDataWithMeta(
                okOrder.assetPair.amountAsset,
                amountAssetDesc.name,
                HttpAssetInfo(amountAssetDesc.decimals).some,
                okOrder.assetPair.priceAsset,
                priceAssetDesc.name,
                HttpAssetInfo(priceAssetDesc.decimals).some,
                0L,
                None,
                HttpMatchingRules(0.1)
              )
            )
          )
        )
      }
    }
  }

  // deleteOrderBookWithKey
  routePath("/orderbook/{amountAsset}/{priceAsset}") - {

    val (someOrder, _) = orderGenerator.sample.get

    "returns an error if there is no such order book" in test(
      route =>
        Delete(routePath(s"/orderbook/${someOrder.assetPair.amountAssetStr}/${someOrder.assetPair.priceAssetStr}"))
          .withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.ServiceUnavailable
        },
      apiKey
    )

    "returns success message if order book exists" in test(
      route =>
        Delete(routePath(s"/orderbook/${okOrder.assetPair.amountAssetStr}/${okOrder.assetPair.priceAssetStr}"))
          .withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.Accepted
          responseAs[HttpMessage] should matchTo(HttpMessage("Deleting order book"))
        },
      apiKey
    )
  }

  // getTransactionsByOrderId
  routePath("/transactions/{orderId}") - {
    "returns known transactions with this order" in test { route =>
      Get(routePath(s"/transactions/${okOrder.idStr()}")) ~> route ~> check {
        responseAs[JsArray].value should have size 1 // we don't have deserializer for domain exchange transaction (only for WavesJ tx)
      }
    }
  }

  // forceCancelOrder
  routePath("/orders/cancel/{orderId}") - {

    "X-Api-Key is required" in test { route =>
      Post(routePath(s"/orders/cancel/${okOrder.id()}")) ~> route ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    "single cancel with API key" - {

      "X-User-Public-Key isn't required, returns that an order was canceled" in test(
        route =>
          Post(routePath(s"/orders/cancel/${okOrder.id()}")).withHeaders(apiKeyHeader) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[HttpSuccessfulSingleCancel] should matchTo(HttpSuccessfulSingleCancel(okOrder.id()))
          },
        apiKey
      )

      "X-User-Public-Key is specified, but wrong" in test(
        route =>
          Post(routePath(s"/orders/cancel/${okOrder.id()}"))
            .withHeaders(apiKeyHeader, RawHeader("X-User-Public-Key", matcherKeyPair.publicKey.base58)) ~> route ~> check {
            status shouldEqual StatusCodes.BadRequest
          },
        apiKey
      )

      "returns an error" in test(
        route =>
          Post(routePath(s"/orders/cancel/${badOrder.id()}")).withHeaders(apiKeyHeader) ~> route ~> check {
            status shouldEqual StatusCodes.BadRequest
            responseAs[HttpError] should matchTo(
              HttpError(
                error = OrderNotFound.code,
                message = s"The order ${badOrder.id()} not found",
                template = "The order {{id}} not found",
                params = Json.obj("id" -> badOrder.id()),
                status = "OrderCancelRejected"
              )
            )
          },
        apiKey
      )

      "sunny day" in test(
        route =>
          Post(routePath(s"/orders/cancel/${okOrder.id()}"))
            .withHeaders(RawHeader("X-API-KEY", apiKey), RawHeader("X-User-Public-Key", okOrder.senderPublicKey.base58)) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[HttpSuccessfulSingleCancel] should matchTo(HttpSuccessfulSingleCancel(okOrder.id()))
          },
        apiKey
      )
    }
  }

  // upsertAssetRate, deleteAssetRate
  routePath("/settings/rates/{assetId}") - {

    val rateCache = RateCache(TestRateDb()).futureValue

    val rate = 0.0055
    val updatedRate = 0.0067

    "add rate" in test(
      route =>
        Put(routePath(s"/settings/rates/$smartAssetId"), rate).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[HttpMessage] should matchTo(HttpMessage(s"The rate $rate for the asset $smartAssetId added"))
          rateCache.getAllRates(smartAsset) shouldBe rate
        },
      apiKey,
      Some(rateCache)
    )

    "update rate" in test(
      route =>
        Put(routePath(s"/settings/rates/$smartAssetId"), updatedRate).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpMessage] should matchTo(
            HttpMessage(s"The rate for the asset $smartAssetId updated, old value = $rate, new value = $updatedRate")
          )
          rateCache.getAllRates(smartAsset) shouldBe updatedRate
        },
      apiKey,
      Some(rateCache)
    )

    "update rate incorrectly (incorrect body)" in test(
      route =>
        Put(
          routePath(s"/settings/rates/$smartAssetId"),
          HttpEntity(ContentTypes.`application/json`, "qwe")
        ).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[HttpMessage] should matchTo(HttpMessage("The provided JSON is invalid. Check the documentation"))
        },
      apiKey,
      Some(rateCache)
    )

    "update rate incorrectly (incorrect value)" in test(
      route =>
        Put(routePath(s"/settings/rates/$smartAssetId"), 0).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[HttpMessage] should matchTo(HttpMessage("Asset rate should be positive and should fit into double"))
        },
      apiKey,
      Some(rateCache)
    )

    "update rate incorrectly (incorrect content type)" in test(
      route =>
        Put(routePath(s"/settings/rates/$smartAssetId"), HttpEntity(ContentTypes.`text/plain(UTF-8)`, "5"))
          .withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[HttpMessage] should matchTo(HttpMessage("The provided Content-Type is not supported, please provide JSON"))
        },
      apiKey,
      Some(rateCache)
    )

    "delete rate" in test(
      route =>
        Delete(routePath(s"/settings/rates/$smartAssetId")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[HttpMessage] should matchTo(HttpMessage(s"The rate for the asset $smartAssetId deleted, old value = $updatedRate"))
          rateCache.getAllRates.keySet should not contain smartAsset
        },
      apiKey,
      Some(rateCache)
    )

    "changing waves rate" in test(
      route =>
        Put(routePath("/settings/rates/WAVES"), rate).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldBe StatusCodes.BadRequest
          responseAs[HttpMessage] should matchTo(HttpMessage("The rate for WAVES cannot be changed"))
        },
      apiKey
    )

    "change rates without api key" in test(
      route =>
        Put(routePath("/settings/rates/WAVES"), rate) ~> route ~> check {
          status shouldBe StatusCodes.Forbidden
          responseAs[HttpMessage] should matchTo(HttpMessage("Provided API key is not correct"))
        },
      apiKey
    )

    "change rates with wrong api key" in test(
      route =>
        Put(routePath("/settings/rates/WAVES"), rate).withHeaders(RawHeader("X-API-KEY", "wrongApiKey")) ~> route ~> check {
          status shouldBe StatusCodes.Forbidden
          responseAs[HttpMessage] should matchTo(HttpMessage("Provided API key is not correct"))
        },
      apiKey
    )

    "deleting waves rate" in test(
      route =>
        Delete(routePath("/settings/rates/WAVES")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldBe StatusCodes.BadRequest
          responseAs[HttpMessage] should matchTo(HttpMessage("The rate for WAVES cannot be changed"))
        },
      apiKey
    )

    "delete rates without api key" in test(
      route =>
        Delete(routePath("/settings/rates/WAVES")) ~> route ~> check {
          status shouldBe StatusCodes.Forbidden
          responseAs[HttpMessage] should matchTo(HttpMessage("Provided API key is not correct"))
        },
      apiKey
    )

    "delete rates with wrong api key" in test(
      route =>
        Delete(routePath("/settings/rates/WAVES")).withHeaders(RawHeader("X-API-KEY", "wrongApiKey")) ~> route ~> check {
          status shouldBe StatusCodes.Forbidden
          responseAs[HttpMessage] should matchTo(HttpMessage("Provided API key is not correct"))
        },
      apiKey
    )

    "delete rate for the asset that doesn't have rate" in test(
      { route =>
        rateCache.deleteRate(smartAsset)
        Delete(routePath(s"/settings/rates/$smartAssetId")).withHeaders(apiKeyHeader) ~> route ~> check {
          status shouldBe StatusCodes.NotFound
          responseAs[HttpMessage] should matchTo(HttpMessage(s"The rate for the asset $smartAssetId was not specified"))
        }
      },
      apiKey,
      Some(rateCache)
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
        responseAs[HttpError] should matchTo(
          HttpError(
            error = InvalidJson.code,
            message = "The provided JSON contains invalid fields: /amount. Check the documentation",
            template = "The provided JSON contains invalid fields: {{invalidFields}}. Check the documentation",
            params = Json.obj("invalidFields" -> JsArray(Seq(JsString("/amount")))),
            status = "InvalidJsonResponse"
          )
        )
      }
    }

    "completely invalid JSON" in test { route =>
      val orderJson = "{ I AM THE DANGEROUS HACKER"
      Post(routePath("/orderbook"), HttpEntity(ContentTypes.`application/json`, orderJson)) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[HttpError] should matchTo(
          HttpError(
            error = InvalidJson.code,
            message = "The provided JSON is invalid. Check the documentation",
            template = "The provided JSON is invalid. Check the documentation",
            params = None,
            status = "InvalidJsonResponse"
          )
        )
      }
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    val orderKey = DbKeys.order(okOrder.id())
    db.put(orderKey.keyBytes, orderKey.encode(Some(okOrder)))
  }

  private def test[U](f: Route => U, apiKey: String = "", maybeRateCache: Option[RateCache] = None): U = {
    val rateCache = maybeRateCache.getOrElse(RateCache(TestRateDb()).futureValue)

    val odb = OrderDb.levelDb(settings.orderDb, asyncLevelDb)
    odb.saveOrder(orderToCancel).futureValue

    val addressActor = TestProbe("address")
    addressActor.setAutoPilot { (sender: ActorRef, msg: Any) =>
      val response = msg match {
        case AddressDirectoryActor.Command.ForwardMessage(forwardAddress, msg) =>
          msg match {
            case AddressActor.Query.GetReservedBalance => AddressActor.Reply.GetBalance(Map(Waves -> 350L))
            case PlaceOrder(x, _) => if (x.id() == okOrder.id()) AddressActor.Event.OrderAccepted(x) else error.OrderDuplicate(x.id())

            case AddressActor.Query.GetOrdersStatuses(_, _) =>
              AddressActor.Reply.GetOrderStatuses(List(okOrder.id() -> OrderInfo.v5(LimitOrder(okOrder), OrderStatus.Accepted)))

            case AddressActor.Query.GetOrderStatus(orderId) =>
              if (orderId == okOrder.id()) AddressActor.Reply.GetOrderStatus(OrderStatus.Accepted)
              else Status.Failure(new RuntimeException(s"Unknown order $orderId"))

            case AddressActor.Command.CancelOrder(orderId, Source.Request) =>
              def handleCancelOrder() =
                if (orderId == okOrder.id() || orderId == orderToCancel.id()) AddressActor.Event.OrderCanceled(orderId)
                else error.OrderNotFound(orderId)

              odb.get(orderId).futureValue match {
                case None => handleCancelOrder()
                case Some(order) if order.sender.toAddress == forwardAddress => handleCancelOrder()
                case _ => error.OrderNotFound(orderId)
              }

            case x @ AddressActor.Command.CancelAllOrders(pair, _, Source.Request) =>
              if (pair.contains(badOrder.assetPair)) error.AddressIsBlacklisted(badOrder.sender)
              else if (pair.forall(_ == okOrder.assetPair))
                AddressActor.Event.BatchCancelCompleted(
                  Map(
                    okOrder.id() -> Right(AddressActor.Event.OrderCanceled(okOrder.id())),
                    badOrder.id() -> Left(error.CanNotPersistEvent)
                  )
                )
              else Status.Failure(new RuntimeException(s"Can't handle $x"))

            case AddressActor.Command.CancelOrders(ids, Source.Request) =>
              AddressActor.Event.BatchCancelCompleted(
                ids.map { id =>
                  id -> (if (id == orderToCancel.id()) Right(AddressActor.Event.OrderCanceled(okOrder.id())) else Left(error.CanNotPersistEvent))
                }.toMap
              )

            case GetTradableBalance(xs) => AddressActor.Reply.GetBalance(xs.map(_ -> 100L).toMap)

            case _: AddressActor.Query.GetOrderStatusInfo =>
              AddressActor.Reply.GetOrdersStatusInfo(OrderInfo.v5(LimitOrder(orderToCancel), OrderStatus.Accepted).some)

            case x => Status.Failure(new RuntimeException(s"Unknown command: $x"))
          }

        case x => Status.Failure(new RuntimeException(s"Unknown message: $x"))
      }

      sender ! response
      TestActor.KeepRunning
    }

    val orderBookDirectoryActor = TestProbe("matcher")
    orderBookDirectoryActor.setAutoPilot { (sender: ActorRef, msg: Any) =>
      msg match {
        case GetSnapshotOffsets =>
          sender ! SnapshotOffsetsResponse(
            Map(
              AssetPair(Waves, priceAsset) -> Some(100L),
              smartWavesPair -> Some(120L),
              AssetPair(smartAsset, priceAsset) -> None
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
            case _ => None
          }

          val entity =
            HttpOrderBook(
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
                HttpOrderBook.toJson(entity)
              )
            )

          request.client ! httpResponse

        case request: AggregatedOrderBookActor.Query.GetMarketStatus => request.client ! smartWavesMarketStatus
        case _ =>
      }

      TestActor.KeepRunning
    }

    val exchangeTxStorage = ExchangeTxStorage.levelDB(asyncLevelDb)
    exchangeTxStorage.put(
      ExchangeTransactionV2
        .create(
          matcherKeyPair.privateKey,
          okOrder.updateType(BUY),
          okOrder.updateType(SELL),
          okOrder.amount,
          okOrder.price,
          okOrder.matcherFee,
          okOrder.matcherFee,
          0.003.waves,
          System.currentTimeMillis
        )
        .explicitGet()
    )

    val testKit = ActorTestKit()
    val orderBooks = new AtomicReference(Map(smartWavesPair -> orderBookActor.ref.asRight[Unit]))
    val orderBookAskAdapter = new OrderBookAskAdapter(orderBooks, 5.seconds)

    val orderBookHttpInfo =
      new OrderBookHttpInfo(
        settings = settings.orderBookHttp,
        askAdapter = orderBookAskAdapter,
        time = time,
        assetDecimals = x =>
          if (x == smartAsset) liftValueAsync(smartAssetDesc.decimals)
          else liftFutureAsync(Future.failed(new IllegalArgumentException(s"No information about $x")))
      )

    val pairBuilder = new AssetPairBuilder(
      settings,
      {
        case `smartAsset` => liftValueAsync[BriefAssetDescription](smartAssetDesc)
        case x if x == okOrder.assetPair.amountAsset || x == badOrder.assetPair.amountAsset || x == unknownAsset =>
          liftValueAsync[BriefAssetDescription](amountAssetDesc)
        case x if x == okOrder.assetPair.priceAsset || x == badOrder.assetPair.priceAsset =>
          liftValueAsync[BriefAssetDescription](priceAssetDesc)
        case x => liftErrorAsync[BriefAssetDescription](error.AssetNotFound(x))
      },
      Set.empty
    )

    val placeRoute = new PlaceRoute(
      pairBuilder,
      addressActor.ref,
      orderValidator = {
        case x if x == okOrder || x == badOrder => liftValueAsync(x)
        case _ => liftErrorAsync(error.FeatureNotImplemented)
      },
      settings,
      () => MatcherStatus.Working,
      Some(crypto secureHash apiKey)
    )
    val cancelRoute = new CancelRoute(pairBuilder, addressActor.ref, () => MatcherStatus.Working, odb, Some(crypto secureHash apiKey), settings)
    val ratesRoute = new RatesRoute(
      pairBuilder,
      () => MatcherStatus.Working,
      Some(crypto secureHash apiKey),
      rateCache,
      testKit.spawn(WsExternalClientDirectoryActor(), s"ws-external-cd-${Random.nextInt(Int.MaxValue)}")
    )
    val historyRoute = new HistoryRoute(pairBuilder, addressActor.ref, () => MatcherStatus.Working, Some(crypto secureHash apiKey), settings)
    val statusRoute = new StatusRoute(pairBuilder, addressActor.ref, () => MatcherStatus.Working, odb, Some(crypto secureHash apiKey), settings)
    val balancesRoute = new BalancesRoute(pairBuilder, addressActor.ref, () => MatcherStatus.Working, Some(crypto secureHash apiKey), settings)
    val transactionsRoute = new TransactionsRoute(() => MatcherStatus.Working, odb, Some(crypto secureHash apiKey))
    val debugRoute = new DebugRoute(
      ConfigFactory.load().atKey("waves.dex"),
      orderBookDirectoryActor.ref,
      addressActor.ref,
      CombinedStream.Status.Working(10),
      () => MatcherStatus.Working,
      () => 0L,
      () => Future.successful(0L),
      Some(crypto secureHash apiKey),
      settings
    )

    val marketsRoute = new MarketsRoute(
      pairBuilder,
      matcherKeyPair.publicKey,
      orderBookDirectoryActor.ref,
      {
        case ValidatedCommand.DeleteOrderBook(pair, _) if pair == okOrder.assetPair =>
          Future.successful(ValidatedCommandWithMeta(1L, System.currentTimeMillis, ValidatedCommand.DeleteOrderBook(pair)).some)
        case _ => Future.failed(new NotImplementedError("Storing is not implemented"))
      },
      {
        case x if x == okOrder.assetPair || x == badOrder.assetPair => Some(Right(orderBookActor.ref))
        case _ => None
      },
      orderBookHttpInfo,
      _ => liftValueAsync(BigDecimal(0.1)),
      settings,
      () => MatcherStatus.Working,
      Some(crypto secureHash apiKey)
    )
    val infoRoute = new InfoRoute(
      matcherKeyPair.publicKey,
      settings,
      () => MatcherStatus.Working,
      300000L,
      Some(crypto secureHash apiKey),
      rateCache,
      () => Future.successful(Set(1, 2, 3)),
      () => DynamicSettings.symmetric(matcherFee)
    )

    val routes = Set(
      cancelRoute.route,
      ratesRoute.route,
      historyRoute.route,
      statusRoute.route,
      balancesRoute.route,
      transactionsRoute.route,
      debugRoute.route,
      marketsRoute.route,
      infoRoute.route
    )

    f(placeRoute.concat(placeRoute.route, routes))
  }

}
