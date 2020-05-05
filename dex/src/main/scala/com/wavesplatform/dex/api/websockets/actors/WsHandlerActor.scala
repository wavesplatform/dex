package com.wavesplatform.dex.api.websockets.actors

import akka.actor.Cancellable
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.{actor => classic}
import cats.syntax.option._
import com.wavesplatform.dex.api.websockets._
import com.wavesplatform.dex.domain.account.AddressScheme
import com.wavesplatform.dex.error.{MatcherError, WsConnectionMaxLifetimeExceeded, WsConnectionPongTimeout}
import com.wavesplatform.dex.market.{AggregatedOrderBookActor, MatcherActor}
import com.wavesplatform.dex.time.Time
import com.wavesplatform.dex.{AddressActor, AddressDirectory, AssetPairBuilder, error}
import shapeless.{Inl, Inr}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object WsHandlerActor {

  sealed trait Message extends Product with Serializable

  sealed trait Command extends Message
  object Command {
    case class ProcessClientMessage(wsMessage: WsClientMessage) extends Command
    case class ProcessClientError(error: Throwable)             extends Command
    case object Stop                                            extends Command

    private[WsHandlerActor] case object SendPing                             extends Command
    private[WsHandlerActor] case class CloseConnection(reason: MatcherError) extends Command
  }

  final case class Settings(pingInterval: FiniteDuration, pongTimeout: FiniteDuration, jwtPublicKey: String)

  // TODO maxConnectionLifetime
  def apply(settings: Settings,
            maxConnectionLifetime: FiniteDuration,
            time: Time,
            assetPairBuilder: AssetPairBuilder,
            clientRef: ActorRef[WsServerMessage],
            matcherRef: classic.ActorRef,
            addressRef: classic.ActorRef): Behavior[Message] =
    Behaviors.setup[Message] { context =>
      import context.executionContext
      context.setLoggerName(s"WebSocketHandlerActor[c=${clientRef.path.name}]")

      def scheduleOnce(delay: FiniteDuration, message: Message): Cancellable = context.scheduleOnce(delay, context.self, message)

      val maxLifetimeExceeded = scheduleOnce(maxConnectionLifetime, Command.CloseConnection(WsConnectionMaxLifetimeExceeded))
      val firstPing           = scheduleOnce(settings.pingInterval, Command.SendPing)

      def schedulePongTimeout(): Cancellable = scheduleOnce(settings.pongTimeout, Command.CloseConnection(WsConnectionPongTimeout))

      def sendPingAndScheduleNextOne(): (WsPingOrPong, Cancellable) = {
        val ping     = WsPingOrPong(time.getTimestamp())
        val nextPing = scheduleOnce(settings.pingInterval, Command.SendPing)
        clientRef ! ping
        ping -> nextPing
      }

      def awaitPong(maybeExpectedPong: Option[WsPingOrPong], pongTimeout: Cancellable, nextPing: Cancellable): Behavior[Message] =
        Behaviors.receiveMessage[Message] {
          case Command.SendPing =>
            val (expectedPong, newNextPing)     = sendPingAndScheduleNextOne()
            val updatedPongTimeout: Cancellable = if (pongTimeout.isCancelled) schedulePongTimeout() else pongTimeout
            awaitPong(expectedPong.some, updatedPongTimeout, newNextPing)

          case Command.ProcessClientMessage(wsMessage) =>
            wsMessage match {
              case pong: WsPingOrPong =>
                maybeExpectedPong match {
                  case None =>
                    context.log.trace("Got unexpected pong: {}", pong)
                    Behaviors.same

                  case Some(expectedPong) =>
                    if (pong == expectedPong) {
                      pongTimeout.cancel()
                      awaitPong(none, Cancellable.alreadyCancelled, nextPing)
                    } else {
                      context.log.trace("Got outdated pong: {}", pong)
                      Behaviors.same
                    }
                }

              case subscribe: WsOrderBookSubscribe =>
                // TODO DEX-700 test for order book that hasn't been created before
                if (subscribe.depth <= 0) clientRef ! WsError.from(error.RequestArgumentInvalid("depth"), time.getTimestamp())
                else
                  assetPairBuilder.validateAssetPair(subscribe.key).value.onComplete {
                    case Success(Left(e)) => clientRef ! WsError.from(e, time.getTimestamp())
                    case Success(Right(_)) =>
                      matcherRef ! MatcherActor.AggregatedOrderBookEnvelope(
                        subscribe.key,
                        AggregatedOrderBookActor.Command.AddWsSubscription(clientRef)
                      )
                    case Failure(e) =>
                      context.log.warn(s"An error during validation the asset pair ${subscribe.key}", e)
                      clientRef ! WsError.from(error.WavesNodeConnectionBroken, time.getTimestamp())
                  }
                Behaviors.same

              case subscribe: WsAddressSubscribe =>
                subscribe.validate(settings.jwtPublicKey, AddressScheme.current.chainId) match {
                  case Left(e)  => clientRef ! WsError.from(e, time.getTimestamp())
                  case Right(_) => addressRef ! AddressDirectory.Envelope(subscribe.key, AddressActor.WsCommand.AddWsSubscription(clientRef))
                }
                Behaviors.same

              case unsubscribe: WsUnsubscribe =>
                unsubscribe.key match {
                  case Inl(x) =>
                    matcherRef ! MatcherActor.AggregatedOrderBookEnvelope(x, AggregatedOrderBookActor.Command.RemoveWsSubscription(clientRef))
                  case Inr(Inl(x)) =>
                    addressRef ! AddressDirectory.Envelope(x, AddressActor.WsCommand.RemoveWsSubscription(clientRef))
                  case Inr(Inr(_)) =>
                }
                Behaviors.same
            }

          case command: Command.ProcessClientError =>
            context.log.debug("Got an error from the client, stopping: {}", command.error)
            Behaviors.stopped

          case Command.Stop =>
            context.log.debug("Got a stop request, stopping...")
            Behaviors.stopped

          case command: Command.CloseConnection =>
            List(nextPing, pongTimeout, maxLifetimeExceeded, firstPing).foreach { _.cancel() }
            context.log.trace("Got a close request, stopping: {}", command.reason.message.text)
            clientRef ! WsServerMessage.Complete
            Behaviors.stopped
        }

      awaitPong(
        maybeExpectedPong = None,
        pongTimeout = Cancellable.alreadyCancelled,
        nextPing = Cancellable.alreadyCancelled
      )
    }
}
