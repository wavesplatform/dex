package com.wavesplatform.dex.api.ws.routes

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.actor.typed.scaladsl.adapter._
import akka.actor.{ActorRef, typed}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.{Directive0, Route}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.typed.scaladsl.{ActorSink, ActorSource}
import akka.stream.{Materializer, OverflowStrategy}
import akka.{Done, NotUsed}
import cats.syntax.either._
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.api.ws.actors.{WsExternalClientHandlerActor, WsInternalBroadcastActor, WsInternalClientHandlerActor}
import com.wavesplatform.dex.api.ws.protocol._
import com.wavesplatform.dex.api.ws.routes.MatcherWebSocketRoute.CloseHandler
import com.wavesplatform.dex.domain.asset.AssetPair
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.error.{InvalidJson, MatcherIsStopping}
import com.wavesplatform.dex.model.AssetPairBuilder
import com.wavesplatform.dex.settings.WebSocketSettings
import com.wavesplatform.dex.time.Time
import com.wavesplatform.dex.{Matcher, error}
import play.api.libs.json.{Json, Reads}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

case class MatcherWebSocketRoute(wsInternalBroadcastRef: typed.ActorRef[WsInternalBroadcastActor.Command],
                                 addressDirectory: ActorRef,
                                 matcher: ActorRef,
                                 time: Time,
                                 assetPairBuilder: AssetPairBuilder,
                                 orderBook: AssetPair => Option[Either[Unit, ActorRef]],
                                 apiKeyHash: Option[Array[Byte]],
                                 webSocketSettings: WebSocketSettings,
                                 matcherStatus: () => Matcher.Status)(implicit mat: Materializer)
    extends ApiRoute
    with AuthRoute
    with ScorexLogging {

  import mat.executionContext

  private val wsHandlers = ConcurrentHashMap.newKeySet[CloseHandler]()

  override def route: Route = pathPrefix("ws" / "v0") {
    matcherStatusBarrier {
      internalWsRoute ~ commonWsRoute
    }
  }

  private val commonWsRoute: Route = (pathEnd & get) {
    import webSocketSettings.externalClientHandler

    val clientId = UUID.randomUUID().toString

    val client                    = mkSource(clientId)
    val (clientRef, clientSource) = client.preMaterialize()

    val webSocketHandlerRef: typed.ActorRef[WsExternalClientHandlerActor.Message] =
      mat.system.spawn(
        behavior = WsExternalClientHandlerActor(externalClientHandler, time, assetPairBuilder, clientRef, matcher, addressDirectory, clientId),
        name = s"handler-$clientId"
      )

    val closeHandler = new CloseHandler(() => webSocketHandlerRef ! WsExternalClientHandlerActor.Command.CloseConnection(MatcherIsStopping))
    wsHandlers.add(closeHandler)

    val server: Sink[WsExternalClientHandlerActor.Message, NotUsed] =
      ActorSink
        .actorRef[WsExternalClientHandlerActor.Message](
          ref = webSocketHandlerRef,
          onCompleteMessage = WsExternalClientHandlerActor.Event.Completed(().asRight),
          onFailureMessage = e => WsExternalClientHandlerActor.Event.Completed(e.asLeft)
        )
        .named(s"server-$clientId")

    val serverSink: Sink[Message, NotUsed] =
      mkServerSink[WsClientMessage, WsExternalClientHandlerActor.Command](clientId, closeHandler, externalClientHandler.healthCheck.pingInterval / 5) {
        case Right(x) => WsExternalClientHandlerActor.Command.ProcessClientMessage(x)
        case Left(x)  => WsExternalClientHandlerActor.Command.ForwardToClient(x)
      }.to(server)

    val flow: Flow[Message, TextMessage.Strict, NotUsed] = Flow.fromSinkAndSourceCoupled(serverSink, clientSource)
    flow.watchTermination()((_, future) => closeHandler.closeOn(future))
    handleWebSocketMessages(flow)
  }

  private val internalWsRoute: Route = (path("internal") & get) {
    import webSocketSettings.internalClientHandler

    val clientId = UUID.randomUUID().toString

    val client                    = mkSource(clientId)
    val (clientRef, clientSource) = client.preMaterialize()

    val webSocketHandlerRef: typed.ActorRef[WsInternalClientHandlerActor.Message] =
      mat.system.spawn(
        behavior = WsInternalClientHandlerActor(internalClientHandler, time, assetPairBuilder, clientRef, matcher, addressDirectory, clientId),
        name = s"handler-$clientId"
      )

    val closeHandler = new CloseHandler(() => webSocketHandlerRef ! WsInternalClientHandlerActor.Command.CloseConnection(MatcherIsStopping))
    wsHandlers.add(closeHandler)

    wsInternalBroadcastRef ! WsInternalBroadcastActor.Command.Subscribe(webSocketHandlerRef)

    val server: Sink[WsInternalClientHandlerActor.Message, NotUsed] =
      ActorSink
        .actorRef[WsInternalClientHandlerActor.Message](
          ref = webSocketHandlerRef,
          onCompleteMessage = WsInternalClientHandlerActor.Event.Completed(().asRight),
          onFailureMessage = e => WsInternalClientHandlerActor.Event.Completed(e.asLeft)
        )
        .named(s"server-$clientId")

    val serverSink: Sink[Message, NotUsed] =
      mkServerSink[WsPingOrPong, WsInternalClientHandlerActor.Command](clientId, closeHandler, internalClientHandler.healthCheck.pingInterval / 5) {
        case Right(x) => WsInternalClientHandlerActor.Command.ProcessClientMessage(x)
        case Left(x)  => WsInternalClientHandlerActor.Command.ForwardToClient(x)
      }.to(server)

    val flow: Flow[Message, TextMessage.Strict, NotUsed] = Flow.fromSinkAndSourceCoupled(serverSink, clientSource)
    flow.watchTermination()((_, future) => closeHandler.closeOn(future))
    handleWebSocketMessages(flow)
  }

  // From server to client
  private def mkSource(clientId: String): Source[TextMessage.Strict, typed.ActorRef[WsServerMessage]] =
    ActorSource
      .actorRef[WsServerMessage](
        { case WsServerMessage.Complete => },
        PartialFunction.empty,
        100,
        OverflowStrategy.fail
      )
      .named(s"source-$clientId")
      .map(WsMessage.toStrictTextMessage(_)(WsServerMessage.wsServerMessageWrites))
      .watchTermination()(handleTermination[WsServerMessage])

  // From client to server
  private def mkServerSink[Raw: Reads, T](clientId: String, closeHandler: CloseHandler, strictTimeout: FiniteDuration)(f: Either[WsError, Raw] => T) =
    Flow[Message]
      .mapAsync[T](1) {
        case tm: TextMessage =>
          tm.toStrict(strictTimeout)
            .map { message =>
              Json.parse(message.getStrictText).as[Raw].asRight[WsError]
            }
            .recover { case _ => WsError.from(InvalidJson(Nil), time.getTimestamp()).asLeft[Raw] }
            .map(f)

        case bm: BinaryMessage =>
          bm.dataStream.runWith(Sink.ignore)
          Future.failed { new IllegalArgumentException("Binary messages are not supported") }
      }
      .watchTermination() { (notUsed, future) =>
        closeHandler.closeOn(future)
        notUsed
      }
      .named(s"sink-$clientId")

  private def handleTermination[T](client: typed.ActorRef[T], r: Future[Done]): typed.ActorRef[T] = {
    val cn = client.path.name
    r.onComplete {
      case Success(_) => log.trace(s"[c=$cn] WebSocket connection successfully closed")
      case Failure(e) => log.trace(s"[c=$cn] WebSocket connection closed with an error: ${Option(e.getMessage).getOrElse(e.getClass.getName)}")
    }(mat.executionContext)
    client
  }

  def gracefulShutdown(): Future[Unit] = {
    val activeConnections = wsHandlers.asScala.filter(!_.closed.isCompleted)
    log.info(s"Closing ${activeConnections.size} connections")
    activeConnections.foreach(_.close())
    Future.sequence { activeConnections.map(_.closed.future) }.map(_ => ())
  }

  private def matcherStatusBarrier: Directive0 = matcherStatus() match {
    case Matcher.Status.Working  => pass
    case Matcher.Status.Starting => complete(error.MatcherIsStarting.toWsHttpResponse(StatusCodes.ServiceUnavailable))
    case Matcher.Status.Stopping => complete(error.MatcherIsStopping.toWsHttpResponse(StatusCodes.ServiceUnavailable))
  }
}

object MatcherWebSocketRoute {
  private[MatcherWebSocketRoute] class CloseHandler(val close: () => Unit, val closed: Promise[Done] = Promise[Done]) {
    def closeOn(f: Future[Done]): Unit = closed.completeWith(f)
  }
}
