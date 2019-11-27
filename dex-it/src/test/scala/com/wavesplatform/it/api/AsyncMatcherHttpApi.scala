package com.wavesplatform.it.api

import java.net.URL
import java.util.UUID

import com.google.common.primitives.Longs
import com.wavesplatform.account.KeyPair
import com.wavesplatform.common.state.ByteStr
import com.wavesplatform.common.utils.Base58
import com.wavesplatform.crypto
import com.wavesplatform.dex.api.CancelOrderRequest
import com.wavesplatform.dex.queue.QueueEventWithMeta
import com.wavesplatform.http.api_key
import com.wavesplatform.it.api.AsyncHttpApi.NodeAsyncHttpApi
import com.wavesplatform.it.sync.config.MatcherPriceAssetConfig
import com.wavesplatform.it.util.{GlobalTimer, TimerExt}
import com.wavesplatform.it.{Node, api}
import com.wavesplatform.transaction.Asset.Waves
import com.wavesplatform.transaction.assets.exchange.{AssetPair, Order, OrderType}
import com.wavesplatform.transaction.{Asset, Proofs}
import org.asynchttpclient.Dsl.{delete => _delete, get => _get}
import org.asynchttpclient.util.HttpConstants
import org.asynchttpclient.{AsyncCompletionHandler, Request, RequestBuilder, Response}
import org.scalatest.Assertions
import play.api.libs.json.Json.{parse, stringify, toJson}
import play.api.libs.json.{JsObject, Json, Writes}

import scala.collection.immutable.TreeMap
import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object AsyncMatcherHttpApi extends Assertions {

  val DefaultMatcherFee: Int = 300000

  def cancelRequest(sender: KeyPair, orderId: String): CancelOrderRequest = {
    val req = CancelOrderRequest(sender, Some(ByteStr.decodeBase58(orderId).get), None, Array.emptyByteArray)
    val signature = crypto.sign(sender, req.toSign)
    req.copy(signature = signature)
  }

  def batchCancelRequest(sender: KeyPair, timestamp: Long): CancelOrderRequest = {
    val req = CancelOrderRequest(sender, None, Some(timestamp), Array.emptyByteArray)
    val signature = crypto.sign(sender, req.toSign)
    req.copy(signature = signature)
  }

  implicit class MatcherAsyncHttpApi(matcherNode: Node) extends NodeAsyncHttpApi(matcherNode) {

    def matcherApiEndpoint: URL = new URL(s"http://localhost:${matcherNode.nodeExternalPort(matcherNode.config.getInt("waves.dex.port"))}")

    def matcherGet(path: String,
                   f: RequestBuilder => RequestBuilder = identity,
                   statusCode: Int = HttpConstants.ResponseStatusCodes.OK_200,
                   waitForStatus: Boolean = false): Future[Response] =
      retrying(f(_get(s"$matcherApiEndpoint$path")).build(), statusCode = statusCode, waitForStatus = waitForStatus)

    def matcherGetWithApiKey(path: String,
                             f: RequestBuilder => RequestBuilder = identity,
                             statusCode: Int = HttpConstants.ResponseStatusCodes.OK_200,
                             waitForStatus: Boolean = false): Future[Response] = retrying(
      _get(s"$matcherApiEndpoint$path")
        .withApiKey(matcherNode.apiKey)
        .build()
    )

    def matcherGetWithSignature(path: String,
                                sender: KeyPair,
                                timestamp: Long = System.currentTimeMillis(),
                                f: RequestBuilder => RequestBuilder = identity): Future[Response] =
      retrying {
        _get(s"$matcherApiEndpoint$path")
          .setHeader("Timestamp", timestamp)
          .setHeader("Signature", Base58.encode(crypto.sign(sender, sender.publicKey ++ Longs.toByteArray(timestamp))))
          .build()
      }

    def matcherGetStatusCode(path: String, statusCode: Int): Future[MessageMatcherResponse] =
      matcherGet(path, statusCode = statusCode).as[MessageMatcherResponse]

    def matcherPost[A: Writes](path: String, body: A, waitForStatus: Boolean = false): Future[Response] =
      post(
        s"$matcherApiEndpoint$path",
        (rb: RequestBuilder) =>
          rb.setHeader("Content-type", "application/json")
            .setHeader("Accept", "application/json")
            .setBody(stringify(toJson(body))),
        waitForStatus
      )

    def postWithAPiKey(path: String): Future[Response] =
      post(
        s"$matcherApiEndpoint$path",
        (rb: RequestBuilder) =>
          rb.withApiKey(matcherNode.apiKey)
            .setHeader("Content-type", "application/json;charset=utf-8")
      )

    def orderStatus(orderId: String, assetPair: AssetPair, waitForStatus: Boolean = true): Future[MatcherStatusResponse] = {
      matcherGet(s"/matcher/orderbook/${assetPair.toUri}/$orderId", waitForStatus = waitForStatus)
        .as[MatcherStatusResponse]
    }

    def orderStatusExpectInvalidAssetId(orderId: String, assetPair: AssetPair, assetId: String): Future[Boolean] =
      orderStatusExpectInvalidAssetId(orderId, assetPair, assetId, _.message == s"Invalid Asset ID: $assetId")

    def orderStatusExpectInvalidAssetId(orderId: String,
                                        assetPair: AssetPair,
                                        assetId: String,
                                        pred: MessageMatcherResponse => Boolean): Future[Boolean] = {
      matcherGet(s"/matcher/orderbook/${assetPair.toUri}/$orderId") transform {
        case Failure(UnexpectedStatusCodeException(_, _, 404, responseBody)) =>
          Try(parse(responseBody).as[MessageMatcherResponse]) match {
            case Success(mr) if pred(mr) => Success(true)
            case Failure(f) => Failure(new RuntimeException(s"Failed to parse response: $f"))
          }
        case Success(r) => Failure(new RuntimeException(s"Unexpected matcher response: (${r.getStatusCode}) ${r.getResponseBody}"))
        case _ => Failure(new RuntimeException(s"Unexpected failure from matcher"))
      }
    }

    def orderBookExpectInvalidAssetId(assetPair: AssetPair, assetId: String): Future[Boolean] =
      orderBookExpectInvalidAssetId(assetPair, assetId, _.message == s"Invalid Asset ID: $assetId")

    def orderBookExpectInvalidAssetId(assetPair: AssetPair, assetId: String, pred: MessageMatcherResponse => Boolean): Future[Boolean] =
      matcherGet(s"/matcher/orderbook/${assetPair.toUri}") transform {
        case Failure(UnexpectedStatusCodeException(_, _, 404, responseBody)) =>
          Try(parse(responseBody).as[MessageMatcherResponse]) match {
            case Success(mr) if pred(mr) => Success(true)
            case Failure(f) => Failure(new RuntimeException(s"Failed to parse response: $f"))
          }
        case Success(r) => Failure(new RuntimeException(s"Unexpected matcher response: (${r.getStatusCode}) ${r.getResponseBody}"))
        case _ => Failure(new RuntimeException(s"Unexpected failure from matcher"))
      }

    def waitTransactionsByOrder(orderId: String, min: Int, retryInterval: FiniteDuration = 1.second): Future[Seq[ExchangeTransaction]] =
      waitFor[Seq[ExchangeTransaction]](s"At least $min transactions for order $orderId")(
        _.matcherGet(s"/matcher/transactions/$orderId").as[Seq[ExchangeTransaction]],
        _.length >= min,
        retryInterval
      )

    def waitForOrderInBlockchain(orderId: String, retryInterval: FiniteDuration = 1.second): Future[Seq[TransactionInfo]] =
      waitTransactionsByOrder(orderId, 1, retryInterval)
        .flatMap { txs =>
          assert(txs.nonEmpty, s"There is no exchange transaction for $orderId")
          Future.sequence {
            txs.map(tx => waitForTransaction(tx.id, retryInterval))
          }
        }

    def orderBook(assetPair: AssetPair): Future[OrderBookResponse] =
      matcherGet(s"/matcher/orderbook/${assetPair.toUri}").as[OrderBookResponse]

    def orderBook(assetPair: AssetPair, depth: Int): Future[OrderBookResponse] =
      matcherGet(s"/matcher/orderbook/${assetPair.toUri}?depth=$depth").as[OrderBookResponse]

    def deleteOrderBook(assetPair: AssetPair): Future[MessageMatcherResponse] =
      retrying(_delete(s"$matcherApiEndpoint/matcher/orderbook/${assetPair.toUri}").withApiKey(matcherNode.apiKey).build(), statusCode = 202)
        .as[MessageMatcherResponse]

    def marketStatus(assetPair: AssetPair): Future[MarketStatusResponse] =
      matcherGet(s"/matcher/orderbook/${assetPair.toUri}/status").as[MarketStatusResponse]

    def cancelOrder(sender: KeyPair, assetPair: AssetPair, orderId: String): Future[MatcherStatusResponse] =
      matcherPost(s"/matcher/orderbook/${assetPair.toUri}/cancel", cancelRequest(sender, orderId)).as[MatcherStatusResponse]

    def expectCancelRejected(sender: KeyPair, assetPair: AssetPair, orderId: String): Future[Unit] = {
      val requestUri = s"/matcher/orderbook/${assetPair.toUri}/cancel"
      matcherPost(requestUri, cancelRequest(sender, orderId)).transform {
        case Failure(UnexpectedStatusCodeException(_, _, 400, body)) if (Json.parse(body) \ "status").as[String] == "OrderCancelRejected" =>
          Success(())
        case Failure(cause) => Failure(cause)
        case Success(resp) => Failure(UnexpectedStatusCodeException("POST", requestUri, resp.getStatusCode, resp.getResponseBody))
      }
    }

    def cancelOrdersForPair(sender: KeyPair, assetPair: AssetPair, timestamp: Long): Future[MatcherStatusResponse] =
      matcherPost(s"/matcher/orderbook/${assetPair.toUri}/cancel", Json.toJson(batchCancelRequest(sender, timestamp)))
        .as[MatcherStatusResponse]

    def cancelOrdersForPairOnce(sender: KeyPair, assetPair: AssetPair, timestamp: Long): Future[Response] =
      onceWithLoggedBody(
        new RequestBuilder()
          .setMethod("POST")
          .setUrl(s"$matcherApiEndpoint/matcher/orderbook/${assetPair.toUri}/cancel")
          .setHeader("Content-type", "application/json")
          .setHeader("Accept", "application/json")
          .setBody(stringify(Json.toJson(batchCancelRequest(sender, timestamp))))
          .build(),
      )

    def onceWithLoggedBody(r: Request): Future[Response] = {
      val id = UUID.randomUUID()
      n.log.trace(s"[$id] Executing request ${r.getMethod} ${r.getUrl}")
      n.client
        .executeRequest(
          r,
          new AsyncCompletionHandler[Response] {
            override def onCompleted(response: Response): Response = {
              n.log.debug(s"[$id] Response for ${r.getUrl} is ${response.getStatusCode}\n${Option(response.getResponseBody).getOrElse("<null>")}")
              response
            }
          }
        )
        .toCompletableFuture
        .toScala
    }

    def cancelAllOrders(sender: KeyPair, timestamp: Long): Future[MatcherStatusResponse] =
      matcherPost(s"/matcher/orderbook/cancel", Json.toJson(batchCancelRequest(sender, timestamp))).as[MatcherStatusResponse]

    def cancelOrderWithApiKey(orderId: String): Future[MatcherStatusResponse] =
      postWithAPiKey(s"/matcher/orders/cancel/$orderId").as[MatcherStatusResponse]

    def fullOrdersHistory(sender: KeyPair, activeOnly: Option[Boolean] = None): Future[Seq[OrderbookHistory]] =
      activeOnly match {
        case None =>
          matcherGetWithSignature(s"/matcher/orderbook/${Base58.encode(sender.publicKey)}", sender).as[Seq[OrderbookHistory]]
        case _ =>
          matcherGetWithSignature(s"/matcher/orderbook/${Base58.encode(sender.publicKey)}?activeOnly=${activeOnly.get}", sender)
            .as[Seq[OrderbookHistory]]
      }

    def orderHistoryByPair(sender: KeyPair, assetPair: AssetPair, activeOnly: Boolean = false): Future[Seq[OrderbookHistory]] = {
      matcherGetWithSignature(s"/matcher/orderbook/${assetPair.toUri}/publicKey/${Base58.encode(sender.publicKey)}?activeOnly=$activeOnly", sender)
        .as[Seq[OrderbookHistory]]
    }

    def reservedBalance(sender: KeyPair): Future[Map[String, Long]] =
      matcherGetWithSignature(s"/matcher/balance/reserved/${Base58.encode(sender.publicKey)}", sender).as[Map[String, Long]]

    def tradableBalance(sender: KeyPair, assetPair: AssetPair): Future[Map[String, Long]] =
      matcherGet(s"/matcher/orderbook/${assetPair.toUri}/tradableBalance/${sender.toAddress.toString}").as[Map[String, Long]]

    def tradingMarkets(): Future[MatcherMarketDataInfo] = matcherGet(s"/matcher/orderbook").as[MatcherMarketDataInfo]

    def waitOrderStatus(assetPair: AssetPair,
                        orderId: String,
                        expectedStatus: String,
                        retryInterval: FiniteDuration = 5.second): Future[MatcherStatusResponse] = {
      waitFor[MatcherStatusResponse](
        s"order(amountAsset=${assetPair.amountAsset}, priceAsset=${assetPair.priceAsset}, orderId=$orderId) status == $expectedStatus")(
        _.orderStatus(orderId, assetPair),
        _.status == expectedStatus,
        retryInterval)
    }

    def waitOrderStatusAndAmount(assetPair: AssetPair,
                                 orderId: String,
                                 expectedStatus: String,
                                 expectedFilledAmount: Option[Long],
                                 retryInterval: FiniteDuration = 1.second): Future[MatcherStatusResponse] = {
      waitFor[MatcherStatusResponse](
        s"order(amountAsset=${assetPair.amountAsset}, priceAsset=${assetPair.priceAsset}, orderId=$orderId) status == $expectedStatus")(
        _.orderStatus(orderId, assetPair),
        s => s.status == expectedStatus && s.filledAmount == expectedFilledAmount,
        5.seconds)
    }

    def prepareOrder(sender: KeyPair,
                     pair: AssetPair,
                     orderType: OrderType,
                     amount: Long,
                     price: Long,
                     fee: Long,
                     version: Byte,
                     timestamp: Long = System.currentTimeMillis,
                     timeToLive: Duration = 30.days - 1.seconds,
                     feeAsset: Asset = Waves): Order = {
      val timeToLiveTimestamp = timestamp + timeToLive.toMillis
      val unsigned =
        Order(sender, MatcherPriceAssetConfig.matcher, pair, orderType, amount, price, timestamp, timeToLiveTimestamp, fee, Proofs.empty, version, feeAsset)
      Order.sign(unsigned, sender)
    }

    def placeOrder(order: JsObject): Future[MatcherResponse] =
      matcherPost("/matcher/orderbook", order).as[MatcherResponse]

    def placeOrder(order: Order): Future[MatcherResponse] =
      matcherPost("/matcher/orderbook", order.json()).as[MatcherResponse]

    def placeMarketOrder(order: Order): Future[MatcherResponse] =
      matcherPost("/matcher/orderbook/market", order.json()).as[MatcherResponse]

    def placeMarketOrder(sender: KeyPair,
                         pair: AssetPair,
                         orderType: OrderType,
                         amount: Long,
                         price: Long,
                         fee: Long,
                         version: Byte,
                         timeToLive: Duration = 30.days - 1.seconds,
                         feeAsset: Asset = Waves,
                         timestamp: Long = System.currentTimeMillis): Future[MatcherResponse] = {
      val order = prepareOrder(sender, pair, orderType, amount, price, fee, version, timeToLive = timeToLive, feeAsset = feeAsset, timestamp = timestamp)
      matcherPost("/matcher/orderbook/market", order.json()).as[MatcherResponse]
    }

    def placeOrder(sender: KeyPair,
                   pair: AssetPair,
                   orderType: OrderType,
                   amount: Long,
                   price: Long,
                   fee: Long,
                   version: Byte,
                   timeToLive: Duration = 30.days - 1.seconds,
                   feeAsset: Asset = Waves,
                   timestamp: Long = System.currentTimeMillis): Future[MatcherResponse] = {
      val order = prepareOrder(sender, pair, orderType, amount, price, fee, version, timeToLive = timeToLive, feeAsset = feeAsset, timestamp = timestamp)
      matcherPost("/matcher/orderbook", order.json()).as[MatcherResponse]
    }

    def expectIncorrectOrderPlacement(order: Order,
                                      expectedStatusCode: Int,
                                      expectedStatus: String,
                                      expectedMessage: Option[String]): Future[Boolean] =
      matcherPost("/matcher/orderbook", order.json()) transform {
        case Failure(UnexpectedStatusCodeException(_, _, `expectedStatusCode`, responseBody)) =>
          expectedMessage match {
            case None =>
              Try(parse(responseBody).as[MatcherStatusResponse]) match {
                case Success(mr) if mr.status == expectedStatus => Success(true)
                case Failure(f) => Failure(new RuntimeException(s"Failed to parse response: $f"))
              }
            case _ =>
              Try(parse(responseBody).as[MatcherErrorResponse]) match {
                case Success(mr) if mr.status.get == expectedStatus && mr.message == expectedMessage =>
                  Success(true)
                case Failure(f) =>
                  Failure(new RuntimeException(s"Failed to parse response: $f"))
              }

          }
        case Success(r) => Failure(new RuntimeException(s"Unexpected matcher response: (${r.getStatusCode}) ${r.getResponseBody}"))
        case _ => Failure(new RuntimeException(s"Unexpected failure from matcher"))
      }

    def ordersByAddress(sender: KeyPair, activeOnly: Boolean): Future[Seq[OrderbookHistory]] =
      matcherGetWithApiKey(s"/matcher/orders/${sender.toAddress.toString}?activeOnly=$activeOnly").as[Seq[OrderbookHistory]]

    def getCurrentOffset: Future[QueueEventWithMeta.Offset] = matcherGetWithApiKey("/matcher/debug/currentOffset").as[QueueEventWithMeta.Offset]

    def getLastOffset: Future[QueueEventWithMeta.Offset] = matcherGetWithApiKey("/matcher/debug/lastOffset").as[QueueEventWithMeta.Offset]

    def getOldestSnapshotOffset: Future[QueueEventWithMeta.Offset] =
      matcherGetWithApiKey("/matcher/debug/oldestSnapshotOffset").as[QueueEventWithMeta.Offset]

    def getAllSnapshotOffsets: Future[Map[String, QueueEventWithMeta.Offset]] =
      matcherGetWithApiKey("/matcher/debug/allSnapshotOffsets").as[Map[String, QueueEventWithMeta.Offset]]

    def waitForStableOffset(confirmations: Int, maxTries: Int, interval: FiniteDuration): Future[QueueEventWithMeta.Offset] = {
      def loop(n: Int, currConfirmations: Int, currOffset: QueueEventWithMeta.Offset): Future[QueueEventWithMeta.Offset] =
        if (currConfirmations >= confirmations) Future.successful(currOffset)
        else if (n > maxTries) Future.failed(new IllegalStateException(s"Offset is not stable: $maxTries tries is out"))
        else
          GlobalTimer.instance
            .sleep(interval)
            .flatMap(_ => matcherNode.getCurrentOffset)
            .flatMap { freshOffset =>
              if (freshOffset == currOffset) loop(n + 1, currConfirmations + 1, freshOffset)
              else loop(n + 1, 0, freshOffset)
            }

      loop(0, 0, -1)
    }

    def matcherState(assetPairs: Seq[AssetPair], orders: IndexedSeq[Order], accounts: Seq[KeyPair]): Future[MatcherState] =
      for {
        offset <- matcherNode.getCurrentOffset
        snapshots <- matcherNode.getAllSnapshotOffsets
        orderBooks <- Future.traverse(assetPairs)(x => matcherNode.orderBook(x).zip(matcherNode.marketStatus(x)).map(r => x -> r))
        orderStatuses <- Future.traverse(orders)(x => matcherNode.orderStatus(x.idStr(), x.assetPair).map(r => x.idStr() -> r))
        reservedBalances <- Future.traverse(accounts)(x => matcherNode.reservedBalance(x).map(r => x -> r))
        accountsOrderHistory = accounts.flatMap(a => assetPairs.map(p => a -> p))
        orderHistory <- Future.traverse(accountsOrderHistory) {
          case (account, pair) => matcherNode.orderHistoryByPair(account, pair).map(r => (account, pair, r))
        }
      } yield {
        val orderHistoryMap = orderHistory
          .groupBy(_._1) // group by accounts
          .map {
            case (account, xs) =>
              val assetPairHistory = xs.groupBy(_._2).map { // group by asset pair
                case (assetPair, historyRecords) => assetPair -> historyRecords.flatMap(_._3) // same as historyRecords.head._3
              }

              account -> (TreeMap.empty[AssetPair, Seq[OrderbookHistory]] ++ assetPairHistory)
          }

        clean {
          api.MatcherState(offset,
            TreeMap(snapshots.toSeq: _*),
            TreeMap(orderBooks: _*),
            TreeMap(orderStatuses: _*),
            TreeMap(reservedBalances: _*),
            TreeMap(orderHistoryMap.toSeq: _*))
        }
      }

    private def clean(x: MatcherState): MatcherState = x.copy(
      orderBooks = x.orderBooks.map { case (k, v) => k -> v.copy(_1 = v._1.copy(timestamp = 0L)) }
    )

    def upsertRate(asset: Asset, rate: Double, expectedStatusCode: Int, apiKey: String = matcherNode.apiKey): Future[RatesResponse] = {
      put(
        s"$matcherApiEndpoint/matcher/settings/rates/${AssetPair.assetIdStr(asset)}",
        (rb: RequestBuilder) =>
          rb.withApiKey(apiKey)
            .setHeader("Content-type", "application/json;charset=utf-8")
            .setBody(stringify(toJson(rate))),
        expectedStatusCode
      ).as[RatesResponse]
    }

    def getRates(): Future[Map[Asset, Double]] = matcherGet("/matcher/settings/rates").as[Map[Asset, Double]]

    def deleteRate(asset: Asset, expectedStatusCode: Int = HttpConstants.ResponseStatusCodes.OK_200, apiKey: String = matcherNode.apiKey): Future[RatesResponse] = {
      retrying(
        _delete(s"$matcherApiEndpoint/matcher/settings/rates/${AssetPair.assetIdStr(asset)}").withApiKey(apiKey).build(),
        statusCode = expectedStatusCode
      ).as[RatesResponse]
    }

    def orderbookInfo(assetPair: AssetPair): Future[MatcherOrderbookInfo] = {
      matcherGet(s"/matcher/orderbook/${assetPair.amountAssetStr}/${assetPair.priceAssetStr}/info").as[MatcherOrderbookInfo]
    }
  }

  implicit class RequestBuilderOps(self: RequestBuilder) {
    def withApiKey(x: String): RequestBuilder = self.setHeader(api_key.name, x)
  }

  implicit class AssetPairExt(val p: AssetPair) extends AnyVal {
    def toUri: String = s"${AssetPair.assetIdStr(p.amountAsset)}/${AssetPair.assetIdStr(p.priceAsset)}"
  }

  private implicit val assetPairOrd: Ordering[AssetPair] = Ordering.by[AssetPair, String](_.key)
  private implicit val KeyPairOrd: Ordering[KeyPair] = Ordering.by[KeyPair, String](_.stringRepr)
}
