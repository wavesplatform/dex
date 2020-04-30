package com.wavesplatform.dex.api

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.typed.scaladsl.adapter._
import akka.actor.{ActorRef, typed}
import akka.http.scaladsl.marshalling.ToResponseMarshaller
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.directives.FutureDirectives
import akka.http.scaladsl.server.{Directive0, Directive1, Route, StandardRoute}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.typed.scaladsl.{ActorSource, _}
import akka.stream.{Materializer, OverflowStrategy}
import cats.syntax.either._
import cats.syntax.option._
import com.google.common.primitives.Longs
import com.wavesplatform.dex.api.MatcherWebSocketRoute._
import com.wavesplatform.dex.api.PathMatchers.{AddressPM, AssetPairPM}
import com.wavesplatform.dex.api.http.{ApiRoute, AuthRoute, `X-Api-Key`}
import com.wavesplatform.dex.api.websockets.actors.WebSocketHandlerActor
import com.wavesplatform.dex.api.websockets.{WsClientMessage, WsMessage, WsPingOrPong}
import com.wavesplatform.dex.domain.account.{Address, PublicKey}
import com.wavesplatform.dex.domain.asset.AssetPair
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.bytes.codec.Base58
import com.wavesplatform.dex.domain.crypto
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.error._
import com.wavesplatform.dex.market.{AggregatedOrderBookActor, MatcherActor}
import com.wavesplatform.dex.settings.WebSocketSettings
import com.wavesplatform.dex.{AddressActor, AddressDirectory, AssetPairBuilder, error}
import io.swagger.annotations.Api
import javax.ws.rs.Path
import play.api.libs.json.Json

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
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

  import mat.executionContext

  private implicit val trm: ToResponseMarshaller[MatcherResponse] = MatcherResponse.toResponseMarshaller

  private val completionMatcher: PartialFunction[WsMessage, Unit]   = { case WsMessage.Complete => }
  private val failureMatcher: PartialFunction[WsMessage, Throwable] = PartialFunction.empty

  private def accountUpdatesSource(address: Address): Source[TextMessage.Strict, typed.ActorRef[WsMessage]] = {
    ActorSource
      .actorRef[WsMessage](
        completionMatcher,
        failureMatcher,
        10,
        OverflowStrategy.fail
      )
      .named(UUID.randomUUID.toString)
      .map(_.toStrictTextMessage)
      .mapMaterializedValue { sourceActor =>
        addressDirectory ! AddressDirectory.Envelope(address, AddressActor.WsCommand.AddWsSubscription(sourceActor))
        sourceActor
      }
      .watchTermination()(handleTermination)
  }

  private def orderBookUpdatesSource(pair: AssetPair): Source[TextMessage.Strict, typed.ActorRef[WsMessage]] = {
    ActorSource
      .actorRef[WsMessage](
        completionMatcher,
        failureMatcher,
        10,
        OverflowStrategy.fail
      )
      .named(UUID.randomUUID.toString)
      .map(_.toStrictTextMessage)
      .mapMaterializedValue { sourceActor =>
        matcher ! MatcherActor.AggregatedOrderBookEnvelope(pair, AggregatedOrderBookActor.Command.AddWsSubscription(sourceActor))
        sourceActor
      }
      .watchTermination()(handleTermination)
  }

  private def mkPongFailure(msg: String): Future[Nothing]            = Future.failed { new IllegalArgumentException(msg) }
  private lazy val binaryMessageUnsupportedFailure: Future[Nothing]  = mkPongFailure("Binary messages are not supported")
  private def unexpectedMessageFailure(msg: String): Future[Nothing] = mkPongFailure(s"Got unexpected message instead of pong: $msg")

  private def createStreamFor(source: Source[TextMessage.Strict, typed.ActorRef[WsMessage]], expiration: Option[Long] = None): Route = {

    import webSocketSettings._

    val connectionLifetime = expiration.fold(maxConnectionLifetime) { exp =>
      FiniteDuration(exp - System.currentTimeMillis, TimeUnit.MILLISECONDS).min(maxConnectionLifetime)
    }

    val (sourceActor, matSource) = source.preMaterialize()
    val systemMessagesHandler = mat.system.spawn(
      behavior = WebSocketHandlerActor(webSocketHandler, connectionLifetime, sourceActor, matcher),
      name = UUID.randomUUID().toString
    )

    val sinkActor = ActorSink.actorRef(
      ref = systemMessagesHandler,
      onCompleteMessage = WebSocketHandlerActor.Command.Stop,
      onFailureMessage = e => {
        log.error(s"Got error", e)
        WebSocketHandlerActor.Command.ProcessClientError(e)
      }
    )

    val sink = Flow[Message]
      .mapAsync[WebSocketHandlerActor.Command.ProcessClientMessage](1) {
        case tm: TextMessage =>
          for {
            strictText <- tm.toStrict(webSocketHandler.pingInterval / 5).map(_.getStrictText)
            pong <- Json.parse(strictText).asOpt[WsPingOrPong] match {
              case None    => unexpectedMessageFailure(strictText)
              case Some(x) => Future.successful(WebSocketHandlerActor.Command.ProcessClientMessage(x))
            }
          } yield pong
        case bm: BinaryMessage => bm.dataStream.runWith(Sink.ignore); binaryMessageUnsupportedFailure
      }
      .to(sinkActor)

    handleWebSocketMessages { Flow.fromSinkAndSourceCoupled(sink, matSource) }
  }

  private def mkStreamFor: Route = {

    import webSocketSettings._

    val clientId = UUID.randomUUID().toString
    val client = ActorSource
      .actorRef[WsMessage](
        { case WsMessage.Complete => },
        PartialFunction.empty,
        10,
        OverflowStrategy.fail
      )
      .named(s"source-$clientId")
      .map(_.toStrictTextMessage)
      .mapMaterializedValue { sourceActor =>
        sourceActor
      }
      .watchTermination()(handleTermination)

    val (connectionSource, matClient) = client.preMaterialize()
    val webSocketHandlerRef = mat.system.spawn(
      behavior = WebSocketHandlerActor(webSocketHandler, maxConnectionLifetime, connectionSource, matcher),
      name = s"handler-$clientId"
    )

    val server = ActorSink.actorRef(
      ref = webSocketHandlerRef,
      onCompleteMessage = WebSocketHandlerActor.Command.Stop,
      onFailureMessage = WebSocketHandlerActor.Command.ProcessClientError
    )

    val serverSink = Flow[Message]
      .mapAsync[WebSocketHandlerActor.Command.ProcessClientMessage](1) {
        case tm: TextMessage =>
          for {
            strictText <- tm.toStrict(webSocketHandler.pingInterval / 5).map(_.getStrictText)
            pong <- Json.parse(strictText).asOpt(WsClientMessage.wsClientMessageReads) match {
              case Some(x) => Future.successful(WebSocketHandlerActor.Command.ProcessClientMessage(x))
              case None    => unexpectedMessageFailure(strictText)
            }
          } yield pong
        case bm: BinaryMessage => bm.dataStream.runWith(Sink.ignore); binaryMessageUnsupportedFailure
      }
      .named(s"sink-$clientId")
      .to(server)

    handleWebSocketMessages { Flow.fromSinkAndSourceCoupled(serverSink, matClient) }
  }

  private def respondWithError(me: MatcherError, sc: StatusCode = StatusCodes.BadRequest): StandardRoute = complete(me toWsHttpResponse sc)

  private def signedGet(prefix: String, address: Address): Directive1[AuthParams] = {
    val directive: Directive1[AuthParams] =
      parameters(('p, 't, 's)).tflatMap {
        case (base58PublicKey, timestamp, base58Signature) =>
          (
            for {
              publicKey <- PublicKey.fromBase58String(base58PublicKey).leftMap(_ => UserPublicKeyIsNotValid)
              _         <- Either.cond(publicKey.toAddress == address, (), AddressAndPublicKeyAreIncompatible(address, publicKey))
              signature <- Either.fromTry { Base58.tryDecodeWithLimit(base58Signature) }.leftMap(_ => RequestInvalidSignature)
            } yield {
              val ts  = timestamp.toLong
              val msg = prefix.getBytes(StandardCharsets.UTF_8) ++ publicKey.arr ++ Longs.toByteArray(ts)
              crypto.verify(signature, msg, publicKey) -> AuthParams(ts, signature)
            }
          ) match {
            case Right((true, authParams)) => provide(authParams)
            case Right((false, _))         => respondWithError(RequestInvalidSignature)
            case Left(matcherError)        => respondWithError(matcherError)
          }
      }

    directive.recover { _ =>
      respondWithError(AuthIsRequired)
    }
  }

  /** Requires PublicKey, Timestamp and Signature of [prefix `au`, PublicKey, Timestamp] */
  private def accountUpdates: Route = (path("accountUpdates" / AddressPM) & get) { address =>
    val directive = optionalHeaderValueByName(`X-Api-Key`.name).flatMap { maybeKey =>
      if (maybeKey.isDefined) withAuth.tmap(_ => none[AuthParams]) else signedGet(balanceStreamPrefix, address).map(_.some)
    }
    directive { maybeAuthParams =>
      createStreamFor(accountUpdatesSource(address), maybeAuthParams.map(_.expirationTimestamp))
    }
  }

  private val orderBookRoute: Route = (path("orderbook" / AssetPairPM) & get) { p =>
    withAssetPair(p) { pair =>
      unavailableOrderBookBarrier(pair) {
        createStreamFor(orderBookUpdatesSource(pair))
      }
    }
  }

  private val commonWsRoute: Route = (pathEnd & get) {
    mkStreamFor
  }

  override def route: Route = pathPrefix("ws") {
    commonWsRoute ~ accountUpdates ~ orderBookRoute
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

  private def handleTermination(client: typed.ActorRef[WsMessage], r: Future[Done]): typed.ActorRef[WsMessage] = {
    val cn = client.path.name
    r.onComplete {
      case Success(_) => log.trace(s"[$cn] WebSocket connection successfully closed")
      case Failure(e) => log.trace(s"[$cn] WebSocket connection closed with an error: ${Option(e.getMessage).getOrElse(e.getClass.getName)}")
    }(mat.executionContext)
    client
  }
}

object MatcherWebSocketRoute {

  val balanceStreamPrefix: String = "au"

  final case class AuthParams(expirationTimestamp: Long, signature: ByteStr)
}
