package com.wavesplatform.dex.queue

import com.google.common.primitives.Longs
import com.wavesplatform.dex.actors.address.AddressActor.Command.Source
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.crypto.DigestSize
import com.wavesplatform.dex.domain.order.Order
import com.wavesplatform.dex.model.{LimitOrder, MarketOrder}

sealed trait ValidatedCommand extends Product with Serializable {
  def assetPair: AssetPair
}

object ValidatedCommand {

  case class PlaceOrder(limitOrder: LimitOrder) extends ValidatedCommand {
    override def assetPair: AssetPair = limitOrder.order.assetPair
  }

  case class PlaceMarketOrder(marketOrder: MarketOrder) extends ValidatedCommand {
    override def assetPair: AssetPair = marketOrder.order.assetPair
  }

  case class CancelOrder(assetPair: AssetPair, orderId: Order.Id, source: Source) extends ValidatedCommand
  case class DeleteOrderBook(assetPair: AssetPair) extends ValidatedCommand

  implicit final class Ops(val self: ValidatedCommand) extends AnyVal {

    def assets: Set[Asset] = self match {
      case x: PlaceOrder => x.assetPair.assets + x.limitOrder.order.feeAsset
      case x: PlaceMarketOrder => x.assetPair.assets + x.marketOrder.order.feeAsset
      case x: CancelOrder => x.assetPair.assets
      case x: DeleteOrderBook => x.assetPair.assets
    }

  }

  def toBytes(x: ValidatedCommand): Array[Byte] = x match {
    case PlaceOrder(lo) => (1: Byte) +: lo.order.version +: lo.order.bytes()
    case CancelOrder(assetPair, orderId, source) => (2: Byte) +: (assetPair.bytes ++ orderId.arr ++ sourceToBytes(source))
    case DeleteOrderBook(assetPair) => (3: Byte) +: assetPair.bytes
    case PlaceMarketOrder(mo) => (4: Byte) +: Array.concat(Longs.toByteArray(mo.availableForSpending), Array(mo.order.version), mo.order.bytes())
  }

  def fromBytes(xs: Array[Byte]): ValidatedCommand = xs.head match {
    case 1 => PlaceOrder(LimitOrder(Order.fromBytes(xs(1), xs.slice(2, Int.MaxValue))))
    case 2 =>
      val bodyBytes = xs.tail
      val (assetPair, offset1) = AssetPair.fromBytes(bodyBytes)
      val offset2 = offset1 + DigestSize
      val orderId = ByteStr(bodyBytes.slice(offset1, offset2))
      CancelOrder(assetPair, orderId, bytesToSource(bodyBytes.drop(offset2)))

    case 3 => DeleteOrderBook(AssetPair.fromBytes(xs.tail)._1)
    case 4 =>
      val afs = Longs.fromByteArray(xs.slice(1, 9)); PlaceMarketOrder(MarketOrder(Order.fromBytes(xs(9), xs.slice(10, Int.MaxValue)), afs))
    case x => throw new IllegalArgumentException(s"Unknown command type: $x")
  }

  // Pre-allocated
  private val sourceToBytes: Map[Source, Array[Byte]] = Map(
    Source.NotTracked -> Array.emptyByteArray,
    Source.Request -> Array(1),
    Source.Expiration -> Array(2),
    Source.BalanceTracking -> Array(3)
  )

  private def bytesToSource(xs: Array[Byte]): Source =
    if (xs.isEmpty) Source.NotTracked
    else
      xs.head match {
        case 1 => Source.Request
        case 2 => Source.Expiration
        case 3 => Source.BalanceTracking
        case x => throw new IllegalArgumentException(s"Unknown source type: $x")
      }

}
