package com.wavesplatform.dex.it

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext
import com.wavesplatform.dex.domain.account.AddressScheme
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.order.Order
import com.wavesplatform.wavesj.Transaction
import com.wavesplatform.wavesj.json.WavesJsonMapper
import com.wavesplatform.wavesj.json.deser.TransactionDeserializer
import com.wavesplatform.wavesj.transactions.ExchangeTransaction
import play.api.libs.json._

import scala.util.{Failure, Success}

package object json {

  private val mapper         = new WavesJsonMapper(AddressScheme.current.chainId)
  private val txDeserializer = new TransactionDeserializer(mapper)
  private val jsonFactory    = new JsonFactory()

  private def deserializeTx(json: JsValue): Transaction = {
//    txDeserializer.deserialize(jsonFactory.createParser(json.toString), DefaultDeserializationContext.Impl)
    mapper.readValue(jsonFactory.createParser(json.toString), classOf[Transaction])
  }

  implicit val transactionFormat: Format[Transaction] = Format[Transaction](
    Reads(json => JsSuccess { deserializeTx(json) }),
    Writes(tx => Json.parse(mapper.writeValueAsString(tx)))
  )

  implicit val byteStrFormat: Format[ByteStr] = Format(
    Reads {
      case JsString(str) =>
        ByteStr.decodeBase58(str) match {
          case Success(x) => JsSuccess(x)
          case Failure(e) => JsError(e.getMessage)
        }

      case _ => JsError("Can't read ByteStr")
    },
    Writes(x => JsString(x.toString))
  )

  implicit val exchangeTxReads: Reads[ExchangeTransaction] = transactionFormat.map(_.asInstanceOf[ExchangeTransaction])

  implicit val orderWrites: Writes[Order] = Writes(_.json())

  implicit val assetPairFormat: Format[AssetPair] = Json.format[AssetPair]

  implicit val assetRatesReads: Reads[Map[Asset, Double]] = Reads { json =>
    json.validate[Map[String, Double]].map { assetRates =>
      assetRates.map { case (assetStr, rateValue) => AssetPair.extractAsset(assetStr).get -> rateValue }
    }
  }

  implicit val assetBalancesReads: Reads[Map[Asset, Long]] = Reads.map[Long].map { assetBalances =>
    assetBalances.map { case (assetStr, balanceValue) => AssetPair.extractAsset(assetStr).get -> balanceValue }
  }

  implicit val assetPairOffsetsReads: Reads[Map[AssetPair, Long]] = Reads { json =>
    json.validate[Map[String, Long]].map {
      _.map {
        case (assetPairStr, offset) =>
          val assetPairStrArr = assetPairStr.split("-")
          val assetPair = (
            assetPairStrArr match {
              case Array(amtAssetStr, prcAssetStr) => AssetPair.createAssetPair(amtAssetStr, prcAssetStr)
              case _                               => throw new Exception(s"$assetPairStr (incorrect assets count, expected 2 but got ${assetPairStrArr.size})")
            }
          ) fold (ex => throw new Exception(s"$assetPairStr (${ex.getMessage})"), identity)
          assetPair -> offset
      }
    }
  }
}
