package com.wavesplatform.dex.it.api.websockets

import com.wavesplatform.dex.api.websockets._
import com.wavesplatform.dex.domain.asset.Asset

import scala.reflect.ClassTag

trait WsConnectionOps {
  final implicit class Ops(val self: WsConnection) {
    def collectMessages[T <: WsServerMessage](implicit ct: ClassTag[T]): List[T] = self.messages.collect {
      case x: T if x.getClass == ct.runtimeClass => x
    }

    def pings: List[WsPingOrPong] = collectMessages[WsPingOrPong]

    def orderBookChanges: List[WsOrderBook] = collectMessages[WsOrderBook]

    def addressStateChanges: List[WsAddressState]    = collectMessages[WsAddressState]
    def balanceChanges: List[Map[Asset, WsBalances]] = addressStateChanges.map(_.balances).filter(_.nonEmpty)
    def orderChanges: List[WsOrder]                  = addressStateChanges.flatMap(_.orders)
  }
}
