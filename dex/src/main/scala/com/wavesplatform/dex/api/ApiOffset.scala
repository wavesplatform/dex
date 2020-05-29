package com.wavesplatform.dex.api

import play.api.libs.json.{Format, JsError, JsNumber, JsSuccess}

case class ApiOffset(offset: Long) extends AnyVal

object ApiOffset {
  implicit val apiCurrentOffsetFormat: Format[ApiOffset] = Format(
    {
      case JsNumber(value) => JsSuccess(ApiOffset(value.toLong))
      case x               => JsError(s"Cannot parse $x as ApiOffset")
    },
    aco => JsNumber(aco.offset)
  )

  implicit def long2ApiOffset(l: Long): ApiOffset = ApiOffset(l)
  implicit def int2ApiOffset(i: Int): ApiOffset   = ApiOffset(i)
}
