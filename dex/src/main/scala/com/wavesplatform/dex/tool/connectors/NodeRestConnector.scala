package com.wavesplatform.dex.tool.connectors

import cats.syntax.either._
import com.wavesplatform.dex.cli.ErrorOr
import com.wavesplatform.dex.tool.connectors.Connector.RepeatRequestOptions
import com.wavesplatform.dex.tool.connectors.RestConnector.ErrorOrJsonResponse
import com.wavesplatform.wavesj.Transaction
import com.wavesplatform.wavesj.json.WavesJsonMapper
import play.api.libs.json.jackson.PlayJsonModule
import play.api.libs.json.{JsValue, JsonParserSettings}
import sttp.client._
import sttp.model.MediaType

import scala.annotation.tailrec
import scala.concurrent.duration._

case class NodeRestConnector(target: String, chainId: Byte) extends RestConnector {

  override implicit val repeatRequestOptions: RepeatRequestOptions = {
    val blocksCount = 5
    (
      for {
        currentHeight <- getCurrentHeight
        blocksTs <- getBlockHeadersAtHeightRange((currentHeight - blocksCount).max(0), currentHeight)
          .map(_.map(json => (json \ "timestamp").as[Long]))
          .ensure("0 or 1 blocks have been forged at the moment, try again later")(_.size > 1)
      } yield {
        val timeBetweenBlocks = (blocksTs.zip(blocksTs.tail).map { case (older, newer) => newer - older }.sum / blocksCount * 1.5 / 1000).toInt
        RepeatRequestOptions(timeBetweenBlocks, 1.second)
      }
    ).fold(ex => throw new RuntimeException(s"Could not construct repeat request options: $ex"), identity)
  }

  private val mapper: WavesJsonMapper = new WavesJsonMapper(chainId); mapper.registerModule(new PlayJsonModule(JsonParserSettings()))

  def broadcastTx(tx: Transaction): ErrorOrJsonResponse = mkResponse {
    _.post(uri"$targetUri/transactions/broadcast").body(mapper writeValueAsString tx).contentType(MediaType.ApplicationJson)
  }

  def getTxInfo(txId: String): ErrorOrJsonResponse    = mkResponse { _.get(uri"$targetUri/transactions/info/$txId") }
  def getTxInfo(tx: JsValue): ErrorOrJsonResponse     = getTxInfo { (tx \ "id").as[String] }
  def getTxInfo(tx: Transaction): ErrorOrJsonResponse = getTxInfo(tx.getId.toString)

  def getCurrentHeight: ErrorOr[Long] = mkResponse { _.get(uri"$targetUri/blocks/height") }.map(json => (json \ "height").as[Long])

  def getBlockHeadersAtHeightRange(from: Long, to: Long): ErrorOr[Seq[JsValue]] =
    mkResponse { _.get(uri"$targetUri/blocks/headers/seq/$from/$to") }.map(_.as[Seq[JsValue]])

  @tailrec
  final def waitForHeightArise(): ErrorOr[Long] = getCurrentHeight match {
    case Right(origHeight) => repeatRequest(getCurrentHeight) { _.exists(_ > origHeight) }
    case Left(_)           => waitForHeightArise()
  }
}
