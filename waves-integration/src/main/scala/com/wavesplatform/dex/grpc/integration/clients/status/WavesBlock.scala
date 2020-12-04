package com.wavesplatform.dex.grpc.integration.clients.status

import com.wavesplatform.dex.domain.bytes.ByteStr

case class WavesBlock(ref: BlockRef, reference: ByteStr, changes: BlockchainBalance, tpe: WavesBlock.Type) {

  def diffIndex: DiffIndex = DiffIndex(
    regular = changes.regular.view.mapValues(_.keySet).toMap,
    outLeases = changes.outLeases.keySet
  )

  override def toString: String = s"WavesBlock(id=${ref.id.base58.take(5)}, h=${ref.height}, tpe=$tpe)"

}

object WavesBlock {
  sealed trait Type extends Product with Serializable

  object Type {
    case object FullBlock extends Type
    case object MicroBlock extends Type
  }

}