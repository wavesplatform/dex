package com.wavesplatform.dex.load

import com.google.common.net.HttpHeaders
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.wavesj.json.WavesJsonMapper
import com.wavesplatform.wavesj.matcher.Order
import com.wavesplatform.wavesj.matcher.Order.Type
import com.wavesplatform.wavesj.{ApiJson, AssetPair, PrivateKeyAccount, Transactions}

package object utils extends ScorexLogging {
  val defaultHeaders = Map(
    HttpHeaders.ACCEPT       -> "application/json",
    HttpHeaders.CONNECTION   -> "close",
    HttpHeaders.CONTENT_TYPE -> "application/json"
  )

  def waitForHeightArise(env: Environment): Unit = {
    val h = env.node.getHeight
    while (env.node.getHeight < h + 1) Thread.sleep(5000)
  }

  def mkOrder(env: Environment, acc: PrivateKeyAccount, orderType: Type, amount: Long, price: Long, pair: AssetPair): Order = {
    Transactions.makeOrder(acc, env.matcherPublicKey, orderType, pair, price, amount, System.currentTimeMillis + 60 * 60 * 24 * 20 * 1000, 300000)
  }

  def mkJson(env: Environment, obj: ApiJson): String = new WavesJsonMapper(env.networkByte).writeValueAsString(obj)

  def mkPost(env: Environment, obj: ApiJson, path: String, tag: String = ""): String = {
    val body = mkJson(env, obj)

    val headers = defaultHeaders ++ Map(
      HttpHeaders.HOST           -> env.loadHost,
      HttpHeaders.CONTENT_LENGTH -> body.length.toString,
      "X-API-Key"                -> env.apiKey
    )

    val request = s"POST $path HTTP/1.1\r\n${headers.map { case (k, v) => s"$k: $v" }.mkString("\r\n")}\r\n\r\n$body"

    s"${request.length} ${tag.toUpperCase}\n$request\r\n"
  }
}
