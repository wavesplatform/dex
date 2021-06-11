package com.wavesplatform.dex.api.http.entities

import akka.http.scaladsl.marshalling.{Marshaller, ToResponseMarshaller}
import akka.http.scaladsl.model.{StatusCodes => C, HttpMessage => _, _}
import akka.util.ByteString
import com.wavesplatform.dex.domain.bytes.ByteStr.byteStrFormat
import com.wavesplatform.dex.domain.order.Order
import com.wavesplatform.dex.error
import com.wavesplatform.dex.error.MatcherError
import com.wavesplatform.dex.meta.getSimpleName
import play.api.libs.json._

sealed class MatcherResponse(val statusCode: StatusCode, val content: MatcherResponseContent) {

  def this(code: StatusCode, error: MatcherError) = this(code, MatcherResponseContent.Single(Json.toJsObject(error)))
  def this(code: StatusCode, error: JsObject) = this(code, MatcherResponseContent.Single(error))

  def status: String = getSimpleName(this)
}

object MatcherResponse {

  val toResponseMarshaller: ToResponseMarshaller[MatcherResponse] = Marshaller.opaque(toHttpResponse)
  implicit val matcherResponseWrites: Writes[MatcherResponse] = Writes(toJson)

  def toHttpResponse(x: MatcherResponse): HttpResponse = HttpResponse(
    x.statusCode,
    entity = HttpEntity.Strict(ContentTypes.`application/json`, ByteString(Json.stringify(toJson(x))))
  )

  def toJson(x: MatcherResponse): JsValue = backwardCompatibleWrapper(
    x.status,
    x.statusCode,
    x.content match {
      case MatcherResponseContent.Single(r) => r
      case MatcherResponseContent.Multiple(xs) => Json.obj("message" -> Json.arr(xs.map(toJson)))
    }
  )

  private def backwardCompatibleWrapper(status: String, code: StatusCode, json: JsObject): JsValue =
    Json
      .obj(
        "success" -> (
          code match {
            case _: C.Success => true
            case _ => false
          }
        ),
        "status" -> status
      )
      .deepMerge(
        code match {
          case _: C.Success => JsObject.empty
          case _ => Json.obj("result" -> JsNull) // For a backward compatibility
        }
      )
      .deepMerge(json)

}

sealed trait MatcherResponseContent

object MatcherResponseContent {
  case class Single(content: JsObject) extends MatcherResponseContent
  case class Multiple(content: List[MatcherResponse]) extends MatcherResponseContent
}

case class SimpleResponse(code: StatusCode, js: JsObject) extends MatcherResponse(code, MatcherResponseContent.Single(js))

object SimpleResponse {
  def apply(code: StatusCode, message: String): SimpleResponse = new SimpleResponse(code, Json.toJsObject(HttpMessage(message)))

  def apply[T](response: T, code: StatusCode = C.OK)(implicit writes: OWrites[T]): SimpleResponse =
    new SimpleResponse(code, writes.writes(response))

}

case class OrderCanceled(orderId: Order.Id) extends MatcherResponse(C.OK, Json.obj("orderId" -> orderId))
case class OrderDeleted(orderId: Order.Id) extends MatcherResponse(C.OK, Json.obj("orderId" -> orderId))

case class SimpleErrorResponse(code: StatusCode, error: MatcherError) extends MatcherResponse(code, error)
case class InvalidJsonResponse(error: MatcherError) extends MatcherResponse(C.BadRequest, error)
case class OrderRejected(error: MatcherError) extends MatcherResponse(C.BadRequest, error)
case class OrderCancelRejected(error: MatcherError) extends MatcherResponse(C.BadRequest, error)
case object InvalidSignature extends MatcherResponse(C.BadRequest, error.RequestInvalidSignature)
case class NotImplemented(error: MatcherError) extends MatcherResponse(C.NotImplemented, error)
case class CanNotPersist(error: MatcherError) extends MatcherResponse(C.ServiceUnavailable, error)
case class OrderBookUnavailable(error: MatcherError) extends MatcherResponse(C.ServiceUnavailable, error)
case class OrderBookNotFound(error: MatcherError) extends MatcherResponse(C.NotFound, error)
case object DuringStart extends MatcherResponse(C.ServiceUnavailable, error.MatcherIsStarting)
case object DuringShutdown extends MatcherResponse(C.ServiceUnavailable, error.MatcherIsStopping)
case object TimedOut extends MatcherResponse(C.InternalServerError, error.RequestTimeout)
case class InfoNotFound(error: MatcherError) extends MatcherResponse(C.NotFound, error)
case class WavesNodeUnavailable(error: MatcherError) extends MatcherResponse(C.ServiceUnavailable, error)
case class RateError(error: MatcherError, code: StatusCode = C.BadRequest) extends MatcherResponse(code, error)

case object InternalError
    extends MatcherResponse(C.ServiceUnavailable, MatcherResponseContent.Single(Json.obj("message" -> "Internal server error")))

case class InvalidAddress(reason: String) extends MatcherResponse(C.BadRequest, error.InvalidAddress(reason))
case class InvalidDepth(reason: String) extends MatcherResponse(C.BadRequest, error.InvalidDepth(reason))
case class InvalidPublicKey(reason: String) extends MatcherResponse(C.BadRequest, error.UserPublicKeyIsNotValid(reason))
case class InvalidAsset(asset: String, reason: String) extends MatcherResponse(C.BadRequest, error.InvalidAsset(asset, reason))
case class InvalidBase58String(reason: String) extends MatcherResponse(C.BadRequest, error.InvalidBase58String(reason))
