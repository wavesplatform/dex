package com.wavesplatform.dex.api

import java.nio.charset.StandardCharsets
import java.util.UUID

import akka.Done
import akka.actor.{ActorRef, Status}
import akka.http.scaladsl.marshalling.ToResponseMarshaller
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.directives.FutureDirectives
import akka.http.scaladsl.server.{Directive0, Directive1, Route}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{CompletionStrategy, Materializer, OverflowStrategy}
import com.google.common.primitives.Longs
import com.wavesplatform.dex.api.MatcherWebSocketRoute._
import com.wavesplatform.dex.api.PathMatchers.{AssetPairPM, PublicKeyPM}
import com.wavesplatform.dex.api.http.{ApiRoute, AuthRoute, `X-Api-Key`}
import com.wavesplatform.dex.api.websockets.WsMessage
import com.wavesplatform.dex.api.websockets.actors.PingPongHandler
import com.wavesplatform.dex.api.websockets.statuses.TerminationStatus
import com.wavesplatform.dex.api.websockets.statuses.TerminationStatus.{MaxLifetimeExceeded, PongTimeout}
import com.wavesplatform.dex.domain.account.PublicKey
import com.wavesplatform.dex.domain.asset.AssetPair
import com.wavesplatform.dex.domain.bytes.codec.Base58
import com.wavesplatform.dex.domain.crypto
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.error.RequestInvalidSignature
import com.wavesplatform.dex.market.MatcherActor
import com.wavesplatform.dex.settings.WebSocketSettings
import com.wavesplatform.dex.{AddressActor, AddressDirectory, AssetPairBuilder, error}
import io.swagger.annotations.Api
import javax.ws.rs.Path

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@Path("/ws")
@Api(value = "/web sockets/")
case class MatcherWebSocketRoute(addressDirectory: ActorRef,
                                 matcher: ActorRef,
                                 assetPairBuilder: AssetPairBuilder,
                                 orderBook: AssetPair => Option[Either[Unit, ActorRef]],
                                 apiKeyHash: Option[Array[Byte]],
                                 webSocketSettings: WebSocketSettings)(implicit mat: Materializer)
    extends ApiRoute
    with AuthRoute
    with ScorexLogging {

  private implicit val trm: ToResponseMarshaller[MatcherResponse] = MatcherResponse.toResponseMarshaller
  private implicit val executionContext: ExecutionContext         = mat.executionContext

  private val pingPongHandler = mat.system.actorOf(PingPongHandler.props(webSocketSettings.pingPongSettings))

  private val completionMatcher: PartialFunction[Any, CompletionStrategy] = {

    case Status.Success(terminationStatus: TerminationStatus) =>
      terminationStatus match {
        case PongTimeout(id)         => log.trace(s"[$id] WebSocket has reached pong timeout, closing...")
        case MaxLifetimeExceeded(id) => log.trace(s"[$id] WebSocket has reached max allowed lifetime, closing...")
      }
      CompletionStrategy.draining

    case Status.Success(s: CompletionStrategy) => s
    case Status.Success(_)                     => CompletionStrategy.draining
    case Status.Success                        => CompletionStrategy.draining
  }

  private val failureMatcher: PartialFunction[Any, Throwable] = { case Status.Failure(cause) => cause }

  private def startPingConnection(sourceActor: ActorRef, connectionId: UUID): Unit = {
    pingPongHandler.tell(PingPongHandler.AddConnection(connectionId), sourceActor)
    mat.system.scheduler
      .scheduleOnce(
        webSocketSettings.maxConnectionLifetime,
        pingPongHandler,
        PingPongHandler.CloseConnection(MaxLifetimeExceeded(connectionId))
      )
  }

  private def accountUpdatesSource(publicKey: PublicKey): Source[TextMessage.Strict, Unit] = {
    Source
      .actorRef[WsMessage](
        completionMatcher,
        failureMatcher,
        10,
        OverflowStrategy.fail
      )
      .map(_.toStrictTextMessage)
      .mapMaterializedValue { sourceActor =>
        val connectionId = UUID.randomUUID()
        addressDirectory.tell(AddressDirectory.Envelope(publicKey, AddressActor.AddWsSubscription(connectionId)), sourceActor)
        startPingConnection(sourceActor, connectionId)
        connectionId
      }
      .watchTermination()(handleTermination)
  }

  private def orderBookUpdatesSource(pair: AssetPair): Source[TextMessage.Strict, Unit] =
    Source
      .actorRef[WsMessage](
        completionMatcher,
        failureMatcher,
        10,
        OverflowStrategy.fail
      )
      .map(_.toStrictTextMessage)
      .mapMaterializedValue { sourceActor =>
        val connectionId = UUID.randomUUID()
        matcher.tell(MatcherActor.AddWsSubscription(pair, connectionId), sourceActor)
        startPingConnection(sourceActor, connectionId)
        connectionId
      }
      .watchTermination()(handleTermination)

  private def createStreamFor(source: Source[TextMessage.Strict, Unit]): Route = handleWebSocketMessages(
    Flow.fromSinkAndSourceCoupled(
      sink = Sink.actorRef(
        ref = pingPongHandler,
        onCompleteMessage = (),
        onFailureMessage = _ => Status.Failure(_)
      ),
      source = source
    )
  )

  private def signedGet(prefix: String, publicKey: PublicKey): Directive0 = {
    val invalidSignatureResponse = complete(RequestInvalidSignature toWsHttpResponse StatusCodes.BadRequest)

    val directive: Directive0 =
      parameters(('t, 's)).tflatMap {
        case (timestamp, signature) =>
          Base58
            .tryDecodeWithLimit(signature)
            .map {
              crypto.verify(_, prefix.getBytes(StandardCharsets.UTF_8) ++ publicKey.arr ++ Longs.toByteArray(timestamp.toLong), publicKey)
            } match {
            case Success(true) => pass
            case _             => invalidSignatureResponse
          }
      }

    directive.recover(_ => invalidSignatureResponse)
  }

  /** Requires PublicKey, Timestamp and Signature of [prefix `as`, PublicKey, Timestamp] */
  private def accountUpdates: Route = (path("accountUpdates" / PublicKeyPM) & get) { publicKey =>
    val directive = optionalHeaderValueByName(`X-Api-Key`.name).flatMap { maybeKey =>
      if (maybeKey.isDefined) withAuth else signedGet(balanceStreamPrefix, publicKey)
    }
    directive { createStreamFor(accountUpdatesSource(publicKey)) }
  }

  private val orderBookRoute: Route = (path("orderbook" / AssetPairPM) & get) { p =>
    withAssetPair(p) { pair =>
      unavailableOrderBookBarrier(pair) {
        createStreamFor(orderBookUpdatesSource(pair))
      }
    }
  }

  override def route: Route = pathPrefix("ws") {
    accountUpdates ~ orderBookRoute
  }

  private def withAssetPair(p: AssetPair): Directive1[AssetPair] = {
    FutureDirectives.onSuccess { assetPairBuilder.validateAssetPair(p).value } flatMap {
      case Right(_) => provide(p)
      case Left(e)  => complete { e.toWsHttpResponse(StatusCodes.BadRequest) }
    }
  }

  private def unavailableOrderBookBarrier(p: AssetPair): Directive0 = orderBook(p) match {
    case Some(x) => if (x.isRight) pass else complete(error.OrderBookBroken(p).toWsHttpResponse(StatusCodes.ServiceUnavailable))
    case None    => complete(error.OrderBookStopped(p).toWsHttpResponse(StatusCodes.NotFound))
  }

  private def handleTermination(id: UUID, r: Future[Done]): Unit = {
    r.onComplete {
      case Success(_) => log.trace(s"[$id] WebSocket connection successfully closed")
      case Failure(e) => log.trace(s"[$id] WebSocket connection closed with an error: ${Option(e.getMessage).getOrElse(e.getClass.getName)}")
    }(mat.executionContext)
  }
}

object MatcherWebSocketRoute {
  val balanceStreamPrefix: String = "au"
}
