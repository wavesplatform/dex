package com.wavesplatform.dex.api.routes

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.{ToResponseMarshallable, ToResponseMarshaller}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.directives.FutureDirectives
import akka.http.scaladsl.server.{Directive1, Directives, Route, _}
import akka.pattern.{ask, AskTimeoutException}
import com.google.common.primitives.Longs
import com.wavesplatform.dex._
import com.wavesplatform.dex.actors.address.{AddressActor, AddressDirectoryActor}
import com.wavesplatform.dex.api.http.entities.{InfoNotFound, InvalidAddress, InvalidAsset, InvalidBase58String, InvalidPublicKey, InvalidSignature, MatcherResponse, TimedOut, _}
import com.wavesplatform.dex.api.http.protocol.HttpCancelOrder
import com.wavesplatform.dex.api.http.{entities, ApiMarshallers}
import com.wavesplatform.dex.domain.account.{Address, PublicKey}
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.bytes.codec.Base58
import com.wavesplatform.dex.domain.crypto
import com.wavesplatform.dex.domain.error.ValidationError
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.error.MatcherError
import com.wavesplatform.dex.model.AssetPairBuilder
import kamon.Kamon

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Success

trait ApiRoute extends Directives with ApiMarshallers with ScorexLogging {

  implicit protected val matcherResponseTrm: ToResponseMarshaller[MatcherResponse] = MatcherResponse.toResponseMarshaller

  protected type LogicResponseHandler = PartialFunction[Any, ToResponseMarshallable]

  protected val timer = Kamon.timer("matcher.api-requests")
  protected val placeTimer = timer.withTag("action", "place")

  def route: Route

  def concat(route: Route, rl: Set[Route]): Route =
    rl.fold(route) { (z, i) =>
      z ~ i
    }

  protected val invalidUserPublicKey: StandardRoute = complete(SimpleErrorResponse(StatusCodes.Forbidden, error.UserPublicKeyIsNotValid()))

  protected def signedGet(publicKey: PublicKey): Directive0 =
    (headerValueByName("Timestamp") & headerValueByName("Signature")).tflatMap { case (timestamp, sig) =>
      Base58.tryDecodeWithLimit(sig).map(crypto.verify(_, publicKey ++ Longs.toByteArray(timestamp.toLong), publicKey)) match {
        case Success(true) => pass
        case _ => complete(InvalidSignature)
      }
    }

  protected def withValidAssetPair(pairOrError: Either[ValidationError.InvalidAsset, AssetPair])(f: AssetPair => Route): Route =
    pairOrError.fold(ia => complete(InvalidAsset(ia.asset, ia.reason)), f)

  protected def withAssetPair(
    assetPairBuilder: AssetPairBuilder,
    pairOrError: Either[ValidationError.InvalidAsset, AssetPair],
    redirectToInverse: Boolean = false,
    suffix: String = "",
    formatError: MatcherError => ToResponseMarshallable = InfoNotFound.apply,
    validate: Boolean = true
  ): Directive1[AssetPair] =
    pairOrError match {
      case Right(p) =>
        if (validate)
          FutureDirectives.onSuccess(assetPairBuilder.validateAssetPair(p).value) flatMap {
            case Right(_) => provide(p)
            case Left(e) if redirectToInverse =>
              FutureDirectives.onSuccess(assetPairBuilder.validateAssetPair(p.reverse).value) flatMap {
                case Right(_) => redirect(s"/matcher/orderbook/${p.priceAssetStr}/${p.amountAssetStr}$suffix", StatusCodes.MovedPermanently)
                case Left(_) => complete(formatError(e))
              }
            case Left(e) => complete(formatError(e))
          }
        else provide(p)
      case Left(ia) => complete(InvalidAsset(ia.asset, ia.reason))
    }

  protected def withAsset(assetPairBuilder: AssetPairBuilder, assetOrError: Either[ValidationError.InvalidAsset, Asset]): Directive1[Asset] =
    assetOrError match {
      case Right(a) => FutureDirectives.onSuccess(assetPairBuilder.validateAssetId(a).value) flatMap {
          case Right(_) => provide(a)
          case Left(e) => complete(InfoNotFound(e))
        }
      case Left(ia) => complete(InvalidAsset(ia.asset, ia.reason))
    }

  protected def withOrderId(orderIdOrError: Either[ValidationError.InvalidBase58String, ByteStr])(f: ByteStr => Route): Route =
    orderIdOrError.fold(io => complete(InvalidBase58String(io.reason)), f)

  protected def withPublicKey(publicKeyOrError: Either[ValidationError.InvalidPublicKey, PublicKey])(f: PublicKey => Route): Route =
    publicKeyOrError.fold(ipk => complete(InvalidPublicKey(ipk.reason)), f)

  protected def withAddress(addressOrError: Either[ValidationError.InvalidAddress, Address])(f: Address => Route): Route =
    addressOrError.fold(ia => complete(InvalidAddress(ia.reason)), f)

  protected def withCancelRequest(f: HttpCancelOrder => Route): Route =
    post {
      entity(as[HttpCancelOrder]) { req =>
        if (req.isSignatureValid()) f(req) else complete(InvalidSignature)
      } ~ complete(StatusCodes.BadRequest)
    } ~ complete(StatusCodes.MethodNotAllowed)

  protected val handleUnknownResponse: LogicResponseHandler = { case x =>
    log.error(s"Can't handle $x")
    entities.InternalError
  }

  protected def askMapAddressActor[A: ClassTag](addressActor: ActorRef, sender: Address, msg: AddressActor.Message)(
    f: A => ToResponseMarshallable
  )(implicit
    executionContext: ExecutionContext,
    timeout: akka.util.Timeout
  ): Future[ToResponseMarshallable] =
    (addressActor ? AddressDirectoryActor.Command.ForwardMessage(sender, msg))
      .mapTo[A]
      .map(f)
      .recover { case e: AskTimeoutException =>
        log.error(s"Error processing $msg", e)
        TimedOut
      }

  protected def askAddressActor(addressActor: ActorRef, sender: Address, msg: AddressActor.Message)(handleResponse: LogicResponseHandler)(
    implicit
    executionContext: ExecutionContext,
    timeout: akka.util.Timeout
  ): Future[ToResponseMarshallable] =
    (addressActor ? AddressDirectoryActor.Command.ForwardMessage(sender, msg))
      .map(handleResponse.orElse(handleUnknownResponse))
      .recover { case e: AskTimeoutException =>
        log.error(s"Error processing $msg", e)
        TimedOut
      }

}
