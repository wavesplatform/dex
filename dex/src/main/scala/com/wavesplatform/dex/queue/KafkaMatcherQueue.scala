package com.wavesplatform.dex.queue

import java.util
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Properties, Timer, TimerTask}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.typesafe.config.{Config, ConfigFactory}
import com.wavesplatform.dex.queue.KafkaMatcherQueue.{KafkaProducer, Settings, eventDeserializer}
import com.wavesplatform.dex.queue.MatcherQueue.{IgnoreProducer, Producer}
import com.wavesplatform.dex.queue.QueueEventWithMeta.Offset
import com.wavesplatform.dex.settings.toConfigOps
import com.wavesplatform.utils.ScorexLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization._

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}

class KafkaMatcherQueue(settings: Settings) extends MatcherQueue with ScorexLogging {
  private implicit val executionContext =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2, new ThreadFactoryBuilder().setNameFormat("kafka-%d").build()))

  private val timer = new Timer("kafka-consumer", true)

  private val duringShutdown = new AtomicBoolean(false)

  private val topicPartition                             = new TopicPartition(settings.topic, 0) // Only one partition
  private val topicPartitions: util.List[TopicPartition] = java.util.Collections.singletonList(topicPartition)

  private val consumerConfig = settings.consumer.client
  private val consumer       = new KafkaConsumer[String, QueueEvent](consumerConfig.toProperties, new StringDeserializer, eventDeserializer)
  consumer.assign(topicPartitions)
  private val pollDuration = java.time.Duration.ofMillis(consumerConfig.getLong("fetch.max.duration.ms"))

  // We need a separate metadata consumer because KafkaConsumer is not safe for multi-threaded access
  private val metadataConsumer = {
    val config = ConfigFactory
      .parseString(s"""
         |client.id = metadata-consumer
         |group.id = meta-${consumerConfig.getString("group.id")}""".stripMargin)
      .withFallback(consumerConfig)
    new KafkaConsumer[String, QueueEvent](config.toProperties, new StringDeserializer, eventDeserializer)
  }
  metadataConsumer.assign(topicPartitions)

  private val producer: Producer = {
    val r =
      if (settings.producer.enable) new KafkaProducer(settings.topic, settings.producer.client.toProperties)
      else IgnoreProducer
    log.info(s"Choosing ${r.getClass.getName} producer")
    r
  }

  @volatile private var lastProcessedOffsetInternal = -1L

  override def startConsume(fromOffset: QueueEventWithMeta.Offset, process: Seq[QueueEventWithMeta] => Future[Unit]): Unit = {
    if (fromOffset > 0) consumer.seek(topicPartition, fromOffset)
    else consumer.seekToBeginning(topicPartitions)

    def loop(): Unit =
      timer.schedule(
        new TimerTask {
          override def run(): Unit =
            if (duringShutdown.get()) Future.successful(Unit)
            else {
              val xs = poll()
              process(xs).andThen {
                case _ =>
                  xs.lastOption.foreach(x => lastProcessedOffsetInternal = x.offset)
                  loop()
              }
            }
        },
        0
      )

    loop()
  }

  private def poll(): IndexedSeq[QueueEventWithMeta] =
    try {
      val records = consumer.poll(pollDuration)
      records.asScala.map { record =>
        QueueEventWithMeta(record.offset(), record.timestamp(), record.value())
      }.toIndexedSeq
    } catch {
      case e: WakeupException => if (duringShutdown.get()) IndexedSeq.empty else throw e
      case e: Throwable =>
        log.error(s"Can't consume", e)
        IndexedSeq.empty
    }

  override def storeEvent(event: QueueEvent): Future[Option[QueueEventWithMeta]] = producer.storeEvent(event)

  override def lastProcessedOffset: Offset = lastProcessedOffsetInternal

  override def lastEventOffset: Future[QueueEventWithMeta.Offset] = Future(metadataConsumer.endOffsets(topicPartitions).get(topicPartition) - 1)

  override def close(timeout: FiniteDuration): Unit = {
    duringShutdown.set(true)

    producer.close(timeout)

    val duration = java.time.Duration.ofNanos(timeout.toNanos)
    consumer.close(duration)
    metadataConsumer.close(duration)

    timer.cancel()
  }
}

object KafkaMatcherQueue {
  case class Settings(topic: String, consumer: ConsumerSettings, producer: ProducerSettings)
  case class ConsumerSettings(client: Config)
  case class ProducerSettings(enable: Boolean, client: Config)

  val eventDeserializer: Deserializer[QueueEvent] = new Deserializer[QueueEvent] {
    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}
    override def deserialize(topic: String, data: Array[Byte]): QueueEvent          = QueueEvent.fromBytes(data)
    override def close(): Unit                                                      = {}
  }

  val eventSerializer: Serializer[QueueEvent] = new Serializer[QueueEvent] {
    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}
    override def serialize(topic: String, data: QueueEvent): Array[Byte]            = QueueEvent.toBytes(data)
    override def close(): Unit                                                      = {}
  }

  private class KafkaProducer(topic: String, producerSettings: Properties)(implicit ec: ExecutionContext) extends Producer with ScorexLogging {
    private val producer =
      new org.apache.kafka.clients.producer.KafkaProducer[String, QueueEvent](producerSettings, new StringSerializer, eventSerializer)

    override def storeEvent(event: QueueEvent): Future[Option[QueueEventWithMeta]] = {
      log.trace(s"Storing $event")

      val p = Promise[QueueEventWithMeta]()

      try {
        producer.send(
          new ProducerRecord[String, QueueEvent](topic, event),
          new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception == null) {
                log.debug(s"$event stored, offset=${metadata.offset()}, timestamp=${metadata.timestamp()}")
                p.success(
                  QueueEventWithMeta(
                    offset = metadata.offset(),
                    timestamp = metadata.timestamp(),
                    event = event
                  ))
              } else {
                log.error(s"During storing $event", exception)
                p.failure(exception)
              }
            }
          }
        )
      } catch {
        case e: Throwable => log.error(s"Can't store message $event", e)
      }

      p.future.map(Some(_))
    }

    override def close(timeout: FiniteDuration): Unit = {
      producer.close(java.time.Duration.ofNanos(timeout.toNanos))
    }
  }
}
