package com.wavesplatform.dex.db

import cats.Monad
import cats.implicits._

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import com.google.common.primitives.{Longs, Shorts}
import com.wavesplatform.dex.db.DbKeys._
import com.wavesplatform.dex.db.leveldb.Key
import com.wavesplatform.dex.queue.{ValidatedCommand, ValidatedCommandWithMeta}
import com.wavesplatform.dex.db.leveldb.LevelDb

trait AbstractLocalQueueStore[F[_]] {

  def enqueue(command: ValidatedCommand, timestamp: Long): F[ValidatedCommandWithMeta.Offset]

  def getFrom(offset: ValidatedCommandWithMeta.Offset, maxElements: Int): F[Vector[ValidatedCommandWithMeta]]

  def oldestOffset: F[Option[ValidatedCommandWithMeta.Offset]]

  def newestOffset: F[Option[ValidatedCommandWithMeta.Offset]]

  def dropUntil(offset: ValidatedCommandWithMeta.Offset): F[Unit]

}

class LocalQueueStore[F[_]](levelDb: LevelDb[F])(implicit fa: Monad[F]) extends AbstractLocalQueueStore[F]{

  private val newestIdx = levelDb.get(lqNewestIdx).map(s => new AtomicLong(s))
  private val inMemQueue = new ConcurrentLinkedQueue[ValidatedCommandWithMeta]
  private var startInMemOffset = Option.empty[ValidatedCommandWithMeta.Offset]

  override def enqueue(command: ValidatedCommand, timestamp: Long): F[ValidatedCommandWithMeta.Offset] = {
    newestIdx.flatMap { oldOffset =>
      val offset = oldOffset.incrementAndGet()
      val eventKey: Key[Option[ValidatedCommandWithMeta]] = lpqElement(offset)

      val x = ValidatedCommandWithMeta(offset, timestamp, command)
      levelDb.readWrite { rw =>
        rw.put(eventKey, Some(x))
        rw.put(lqNewestIdx, offset)
      }.map { _ =>
        inMemQueue.add(x)
        if (startInMemOffset.isEmpty) startInMemOffset = Some(offset)
        offset
      }
    }
  }

  override def getFrom(offset: ValidatedCommandWithMeta.Offset, maxElements: Int): F[Vector[ValidatedCommandWithMeta]] =
    if (startInMemOffset.exists(_ <= offset))
      if (inMemQueue.isEmpty) Vector.empty[ValidatedCommandWithMeta].pure[F]
      else {
        val xs = Vector.newBuilder[ValidatedCommandWithMeta]
        var added = 0

        while (!inMemQueue.isEmpty && added < maxElements) Option(inMemQueue.poll()).foreach { x =>
          xs += x
          added += 1
        }

        xs.result()
      }.pure[F]
    else
      levelDb.readOnly { rw =>
        rw.read(LqElementKeyName, LqElementPrefixBytes, lpqElement(math.max(offset, 0)).keyBytes, Int.MaxValue) { e =>
          val offset = Longs.fromByteArray(e.getKey.slice(Shorts.BYTES, Shorts.BYTES + Longs.BYTES))
          lpqElement(offset).parse(e.getValue).getOrElse(throw new RuntimeException(s"Can't find a queue event at $offset"))
        }
      }

  override def oldestOffset: F[Option[ValidatedCommandWithMeta.Offset]] =
    levelDb.readOnly { rw =>
      rw.read(LqElementKeyName, LqElementPrefixBytes, lpqElement(0).keyBytes, 1) { e =>
        Longs.fromByteArray(e.getKey.slice(Shorts.BYTES, Shorts.BYTES + Longs.BYTES))
      }.headOption
    }

  override def newestOffset: F[Option[ValidatedCommandWithMeta.Offset]] = {
    newestIdx.flatMap { oldOffset =>
      val idx = oldOffset.get()
      val eventKey = lpqElement(idx)
      levelDb.get(eventKey).map(_.map(_.offset))
    }
  }

  override def dropUntil(offset: ValidatedCommandWithMeta.Offset): F[Unit] = levelDb.get(lqOldestIdx).flatMap { idx =>
    levelDb.readWrite { rw =>
      val oldestIdx = math.max(idx, 0)
      (oldestIdx until offset).foreach { offset =>
        rw.delete(lpqElement(offset))
      }
      rw.put(lqOldestIdx, offset)
    }
  }

}
