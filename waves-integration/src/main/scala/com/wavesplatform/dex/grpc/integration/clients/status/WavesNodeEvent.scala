package com.wavesplatform.dex.grpc.integration.clients.status

import com.google.protobuf.ByteString
import com.wavesplatform.dex.grpc.integration.services.UtxTransaction
import scorex.util.encode.Base58

sealed trait WavesNodeEvent extends Product with Serializable

object WavesNodeEvent {

  case class Appended(block: WavesBlock, forgedTxIds: Seq[ByteString]) extends WavesNodeEvent {
    override def toString: String = s"Appended(${block.tpe}, h=${block.ref.height}, ${block.ref.id}, ftx=${txIdsToString(forgedTxIds)})"
  }

  // Could also happen on appending of a key block
  // It is expected, that we get Appended after RolledBack
  case class RolledBack(to: RolledBack.To) extends WavesNodeEvent

  object RolledBack {
    sealed trait To extends Product with Serializable

    object To {

      case class CommonBlockRef(ref: BlockRef) extends To {
        override def toString: String = s"CommonBlockRef(${ref.height}, ${ref.id})"
      }

      case class Height(h: Int) extends To

    }

  }

  case class DataReceived(updates: BlockchainBalance) extends WavesNodeEvent {
    override def toString: String = "DataReceived"
  }

  case class UtxAdded(txs: Seq[UtxTransaction]) extends WavesNodeEvent {
    override def toString: String = s"UtxAdded(${txsToString(txs)})"
  }

  case class UtxSwitched(newTxs: Seq[UtxTransaction]) extends WavesNodeEvent {
    override def toString: String = s"UtxSwitched(${txsToString(newTxs)})"
  }

  sealed trait WavesNodeUtxEvent extends Product with Serializable

  object WavesNodeUtxEvent {
    case class Added(txs: Seq[UtxTransaction]) extends WavesNodeUtxEvent
    case class Forged(txIds: Seq[ByteString]) extends WavesNodeUtxEvent
    case class Switched(newTxs: Seq[UtxTransaction]) extends WavesNodeUtxEvent
  }

  // Utility

  implicit final class ByteStringOps(val self: ByteString) extends AnyVal {
    def toBase58: String = Base58.encode(self.toByteArray)
  }

  private def txsToString(txs: Seq[UtxTransaction]): String = txs.map(_.id.toBase58).mkString(", ")
  private def txIdsToString(txs: Seq[ByteString]): String = txs.map(_.toBase58).mkString(", ")

}
