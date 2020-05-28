package com.wavesplatform.dex.load

import com.google.common.net.HttpHeaders
import com.softwaremill.sttp.{HttpURLConnectionBackend, MonadError => _, _}
import com.typesafe.config.ConfigFactory
import com.wavesplatform.dex.domain.bytes.codec.Base58
import com.wavesplatform.wavesj.json.WavesJsonMapper
import com.wavesplatform.wavesj.matcher.Order
import com.wavesplatform.wavesj.matcher.Order.Type
import com.wavesplatform.wavesj.{ApiJson, AssetPair, PrivateKeyAccount, Transactions}

import scala.util.Random

package object utils {
  implicit val settings = new LoadTestSettings(ConfigFactory.parseResources(scala.util.Properties.envOrElse("CONF", "devnet.conf")))
  implicit val backend  = HttpURLConnectionBackend()

  val defaultHeaders = Map(
    HttpHeaders.ACCEPT       -> "application/json",
    HttpHeaders.CONNECTION   -> "close",
    HttpHeaders.CONTENT_TYPE -> "application/json"
  )

  def mkOrderHistoryHeaders(account: PrivateKeyAccount, timestamp: Long = System.currentTimeMillis): Map[String, String] = Map(
    "Timestamp" -> timestamp.toString,
    "Signature" -> settings.matcher.getOrderHistorySignature(account, timestamp)
  )

  def getOrderBook(account: PrivateKeyAccount): String = {
    sttp
      .get(uri"${settings.matcherUrl}/matcher/orderbook/${Base58.encode(account.getPublicKey)}")
      .headers(mkOrderHistoryHeaders(account))
      .send()
      .body
      .right
      .get
  }

  def waitForHeightArise(): Unit = {
    val toHeight = settings.node.getHeight + 1
    println(s"\tWaiting for the next ($toHeight) block...")
    while (settings.node.getHeight < toHeight) Thread.sleep(5000)
  }

  def mkAsset(): String = {
    val tx =
      Transactions.makeIssueTx(
        settings.issuer,
        settings.networkByte,
        Random.nextInt(10000000).toString,
        Random.nextInt(10000000).toString,
        settings.assetQuantity,
        8, //TODO: random from 2 to 16
        false,
        null,
        settings.issueFee
      )
    println(s"\tSending Issue TX: ${mkJson(tx)}")
    settings.node.send(tx)
    tx.getId.toString
  }

  def mkOrder(acc: PrivateKeyAccount, orderType: Type, amount: Long, price: Long, pair: AssetPair): Order = {
    Transactions.makeOrder(acc,
                           settings.matcherPublicKey,
                           orderType,
                           pair,
                           price,
                           amount,
                           System.currentTimeMillis + 60 * 60 * 24 * 20 * 1000,
                           300000)
  }

  def mkJson(obj: ApiJson): String = new WavesJsonMapper(settings.networkByte).writeValueAsString(obj)

  def mkPost(obj: ApiJson, path: String, tag: String = ""): String = {
    val body = mkJson(obj).replace("\"matcherFeeAssetId\":\"WAVES\",", "")

    val headers = defaultHeaders ++ Map(
      HttpHeaders.HOST           -> settings.loadHost,
      HttpHeaders.CONTENT_LENGTH -> body.length.toString,
      "X-API-Key"                -> settings.apiKey
    )

    val request = s"POST $path HTTP/1.1\r\n${headers.map { case (k, v) => s"$k: $v" }.mkString("\r\n")}\r\n\r\n$body"

    s"${request.length} ${tag.toUpperCase}\n$request\r\n"
  }
}