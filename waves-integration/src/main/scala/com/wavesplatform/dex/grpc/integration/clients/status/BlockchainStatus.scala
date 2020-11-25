package com.wavesplatform.dex.grpc.integration.clients.status

import com.wavesplatform.dex.grpc.integration.clients.status.WavesNodeEvent.WavesNodeUtxEvent
import com.wavesplatform.dex.meta.getSimpleName

import scala.collection.immutable.Queue

sealed trait BlockchainStatus extends Product with Serializable {
  def name: String = getSimpleName(this)
}

object BlockchainStatus {

  case class Normal(mainFork: WavesFork, currentHeightHint: Int) extends BlockchainStatus {
    override def toString: String = s"Normal(${mainFork.history.headOption.map(_.ref)})"
  }

  /**
   * @param newFork Required to be non-empty
   */
  case class TransientRollback(
    newFork: WavesFork,
    newForkChanges: BlockchainBalance, // from a common block
    previousForkHeight: Int,
    previousForkDiffIndex: DiffIndex, // from a common block TODO should really be from the common block
    utxEventsStash: Queue[WavesNodeUtxEvent]
  ) extends BlockchainStatus {
    require(newFork.history.nonEmpty, "newFork must not be empty!")

    override def toString: String =
      s"TransientRollback(n=${newFork.history.headOption.map(_.ref)}, h=$previousForkHeight, utx=${utxEventsStash.size})"

  }

  // TODO do we need currentHeightHint
  case class TransientResolving(
    mainFork: WavesFork,
    stash: Queue[WavesNodeEvent],
    currentHeightHint: Int,
    utxEventsStash: Queue[WavesNodeUtxEvent]
  ) extends BlockchainStatus {

    override def toString: String =
      s"TransientResolving(${mainFork.history.headOption.map(_.ref)}, l=${stash.lastOption}, utx=${utxEventsStash.size})"

  }

}
