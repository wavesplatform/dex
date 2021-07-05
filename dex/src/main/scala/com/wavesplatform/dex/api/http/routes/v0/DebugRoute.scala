package com.wavesplatform.dex.api.http.routes.v0

import akka.actor.ActorRef
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server._
import akka.pattern.ask
import akka.stream.Materializer
import akka.util.Timeout
import com.typesafe.config.Config
import com.wavesplatform.dex.actors.OrderBookDirectoryActor._
import com.wavesplatform.dex.actors.address.AddressActor.Query.GetCurrentState
import com.wavesplatform.dex.actors.address.AddressActor.Reply.GetState
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives._
import com.wavesplatform.dex.api.http.directives.ProtectDirective
import com.wavesplatform.dex.api.http.entities._
import com.wavesplatform.dex.api.http.headers.CustomContentTypes
import com.wavesplatform.dex.api.http.{HasStatusBarrier, _}
import com.wavesplatform.dex.api.routes.PathMatchers.AddressPM
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.grpc.integration.clients.combined.CombinedStream
import com.wavesplatform.dex.queue.ValidatedCommandWithMeta
import com.wavesplatform.dex.settings.MatcherSettings
import com.wavesplatform.dex.settings.utils.ConfigOps.ConfigOps
import io.swagger.annotations._

import javax.ws.rs.Path
import scala.concurrent.{ExecutionContext, Future}

@Path("/matcher")
@Api()
class DebugRoute(
  safeConfig: Config,
  matcher: ActorRef,
  addressActor: ActorRef,
  blockchainStatus: => CombinedStream.Status,
  override val matcherStatus: () => MatcherStatus,
  currentOffset: () => ValidatedCommandWithMeta.Offset,
  lastOffset: () => Future[ValidatedCommandWithMeta.Offset],
  override val apiKeyHash: Option[Array[Byte]],
  matcherSettings: MatcherSettings
)(implicit mat: Materializer)
    extends ApiRoute
    with ProtectDirective
    with HasStatusBarrier
    with AuthRoute
    with ScorexLogging {

  implicit private val executionContext: ExecutionContext = mat.executionContext
  implicit private val timeout: Timeout = matcherSettings.actorResponseTimeout

  override lazy val route: Route = pathPrefix("matcher" / "debug") {
    getMatcherStatus ~ getAddressState ~ getMatcherConfig ~ getCurrentOffset ~ getLastOffset ~
    getOldestSnapshotOffset ~ getAllSnapshotOffsets ~ saveSnapshots ~ printMessage
  }

  // Hidden
  def printMessage: Route =
    (path("print") & post) {
      (withMetricsAndTraces("printMessage") & withAuth) {
        entity(as[HttpMessage]) { x =>
          log.warn(x.message)
          complete {
            SimpleResponse(StatusCodes.OK, "Message logged")
          }
        }
      }
    }

  @Path("/debug/config#getMatcherConfig")
  @ApiOperation(
    value = "Returns current matcher's configuration. Requires API Key",
    httpMethod = "GET",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("debug"),
    produces = "application/hocon",
    response = classOf[HttpResponse]
  )
  def getMatcherConfig: Route =
    (path("config") & get) {
      (withMetricsAndTraces("getMatcherConfig") & withAuth) {
        complete {
          HttpEntity(safeConfig.rendered).withContentType(CustomContentTypes.`application/hocon`)
        }
      }
    }

  @Path("/debug/currentOffset#getCurrentOffset")
  @ApiOperation(
    value = "Get the current offset in the queue. Requires API Key",
    httpMethod = "GET",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("debug"),
    response = classOf[HttpOffset]
  )
  def getCurrentOffset: Route =
    (path("currentOffset") & get) {
      (withMetricsAndTraces("getCurrentOffset") & withAuth) {
        complete(currentOffset().toJson)
      }
    }

  @Path("/debug/lastOffset#getLastOffset")
  @ApiOperation(
    value = "Get the last offset in the queue. Requires API Key",
    httpMethod = "GET",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("debug"),
    response = classOf[HttpOffset]
  )
  def getLastOffset: Route =
    (path("lastOffset") & get) {
      (withMetricsAndTraces("getLastOffset") & withAuth) {
        complete(lastOffset() map (_.toJson))
      }
    }

  @Path("/debug/oldestSnapshotOffset#getOldestSnapshotOffset")
  @ApiOperation(
    value = "Get the oldest snapshot's offset in the queue. Requires API Key",
    httpMethod = "GET",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("debug"),
    response = classOf[HttpOffset]
  )
  def getOldestSnapshotOffset: Route =
    (path("oldestSnapshotOffset") & get) {
      (withMetricsAndTraces("getOldestSnapshotOffset") & withAuth) {
        complete {
          (matcher ? GetSnapshotOffsets).mapTo[SnapshotOffsetsResponse].map { response =>
            val defined = response.offsets.valuesIterator.collect { case Some(x) => x }
            (if (defined.isEmpty) -1L else defined.min).toJson
          }
        }
      }
    }

  @Path("/debug/allSnapshotOffsets#getAllSnapshotOffsets")
  @ApiOperation(
    value = "Get all snapshots' offsets in the queue. Requires API Key",
    notes = "Returns Map[Asset Pair, Long]",
    httpMethod = "GET",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("debug"),
    response = classOf[HttpSnapshotOffsets]
  )
  def getAllSnapshotOffsets: Route =
    (path("allSnapshotOffsets") & get) {
      (withMetricsAndTraces("getAllSnapshotOffsets") & withAuth) {
        complete {
          (matcher ? GetSnapshotOffsets).mapTo[SnapshotOffsetsResponse].map { x =>
            x.offsets.collect { case (assetPair, Some(offset)) => assetPair -> offset }.toJson
          }
        }
      }
    }

  @Path("/debug/saveSnapshots#saveSnapshots")
  @ApiOperation(
    value = "Saves snapshots for all Order Books. Requires API Key",
    httpMethod = "POST",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("debug"),
    response = classOf[HttpMessage]
  )
  def saveSnapshots: Route =
    (path("saveSnapshots") & post) {
      (withMetricsAndTraces("saveSnapshots") & protect & withAuth) {
        complete {
          matcher ! ForceSaveSnapshots
          SimpleResponse(StatusCodes.OK, "Saving started")
        }
      }
    }

  @Path("/debug/address/{address}#getAddressState")
  @ApiOperation(
    value = "Get state (balances, placement queue by address). Requires API Key",
    httpMethod = "GET",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("debug"),
    response = classOf[HttpAddressState]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "address", value = "Address", dataType = "string", paramType = "path")
    )
  )
  def getAddressState: Route =
    (path("address" / AddressPM) & get) { addressOrError =>
      (withMetricsAndTraces("getAddressState") & withAuth) {
        withAddress(addressOrError) { address =>
          complete {
            askMapAddressActor[GetState](addressActor, address, GetCurrentState) {
              HttpAddressState(_)
            }
          }
        }
      }
    }

  @Path("/debug/status#getMatcherStatus")
  @ApiOperation(
    value = "Returns current matcher's status. Requires API Key",
    httpMethod = "GET",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("debug"),
    response = classOf[HttpSystemStatus]
  )
  def getMatcherStatus: Route =
    (path("status") & get) {
      (withMetricsAndTraces("getMatcherStatus") & withAuth) {
        complete(HttpSystemStatus(matcherStatus(), blockchainStatus))
      }
    }

}
