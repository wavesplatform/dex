package com.wavesplatform.dex.grpc.integration.clients

import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.grpc.integration.clients.blockchainupdates.{BlockchainUpdatesControlledStream, BlockchainUpdatesConversions}
import com.wavesplatform.dex.grpc.integration.clients.domain.WavesNodeEvent
import com.wavesplatform.dex.grpc.integration.clients.matcherext.{UtxEventConversions, UtxEventsControlledStream}
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject

import scala.concurrent.duration.DurationInt

class CombinedStream(blockchainUpdates: BlockchainUpdatesControlledStream, utxEvents: UtxEventsControlledStream)(implicit scheduler: Scheduler)
    extends ScorexLogging {

  // Note, it is not a processed height! A less value could be emitted to control
  @volatile private var heightHint = 1

  @volatile private var blockchainUpdatesStopped = false
  @volatile private var utxEventsStopped = false

  @volatile private var blockchainUpdatesClosed = false
  @volatile private var utxEventsClosed = false

  private val internalStream = ConcurrentSubject.publish[WavesNodeEvent]
  val stream: Observable[WavesNodeEvent] = internalStream

  blockchainUpdates.systemStream.foreach {
    case ControlledStream.SystemEvent.BecameReady => utxEvents.start()

    case ControlledStream.SystemEvent.Stopped =>
      if (utxEventsStopped) recover()
      else {
        blockchainUpdatesStopped = true
        utxEvents.stop()
      }

    case ControlledStream.SystemEvent.Closed =>
      if (utxEventsClosed) close()
      else {
        blockchainUpdatesClosed = true
        utxEvents.close()
      }
  }

  blockchainUpdates.stream.foreach { evt =>
    evt.update.flatMap(BlockchainUpdatesConversions.toEvent) match {
      case Some(x) => internalStream.onNext(x)
      case None =>
        log.warn(s"Can't convert $evt to a domain event, asking next")
        blockchainUpdates.requestNext()
    }
  }

  utxEvents.systemStream.foreach {
    case ControlledStream.SystemEvent.BecameReady => // ok

    case ControlledStream.SystemEvent.Stopped =>
      if (blockchainUpdatesStopped) recover()
      else {
        utxEventsStopped = true
        blockchainUpdates.stop()
      }

    case ControlledStream.SystemEvent.Closed =>
      if (blockchainUpdatesClosed) close()
      else {
        utxEventsClosed = true
        blockchainUpdates.close()
      }
  }

  utxEvents.stream.foreach { evt =>
    UtxEventConversions.toEvent(evt) match {
      case Some(x) => internalStream.onNext(x)
      case None =>
        log.warn(s"Can't convert $evt to a domain event")
        blockchainUpdates.requestNext()
    }
  }

  def updateHeightHint(height: Int): Unit = heightHint = height

  // TODO TransientRollback during first start to stash utx events
  def startFrom(height: Int): Unit = {
    heightHint = height
    blockchainUpdates.startFrom(height)
  }

  def restartFrom(height: Int): Unit = {
    // TODO do not restart UTX
    heightHint = height
    scheduler.scheduleOnce(1.second)(blockchainUpdates.stop()) // Self-healed above
  }

  private def recover(): Unit = {
    val rollBackHeight = math.max(1, heightHint - 1)
    internalStream.onNext(WavesNodeEvent.RolledBack(WavesNodeEvent.RolledBack.To.Height(rollBackHeight)))
    scheduler.scheduleOnce(1.second) {
      blockchainUpdates.startFrom(heightHint)
    }
  }

  private def close(): Unit = internalStream.onComplete()

}
