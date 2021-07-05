package com.wavesplatform.dex.api.http.routes.v0

import akka.actor.typed
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server._
import akka.stream.Materializer
import com.wavesplatform.dex._
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives._
import com.wavesplatform.dex.api.http.directives.ProtectDirective
import com.wavesplatform.dex.api.http.entities._
import com.wavesplatform.dex.api.http.{HasStatusBarrier, _}
import com.wavesplatform.dex.api.routes.PathMatchers.AssetPM
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.api.ws.actors.WsExternalClientDirectoryActor
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.caches.RateCache
import com.wavesplatform.dex.domain.asset.Asset.Waves
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.model.AssetPairBuilder
import io.swagger.annotations.{Api, _}

import javax.ws.rs.Path

@Path("/matcher")
@Api()
class RatesRoute(
  assetPairBuilder: AssetPairBuilder,
  override val matcherStatus: () => MatcherStatus,
  override val apiKeyHash: Option[Array[Byte]],
  rateCache: RateCache,
  externalClientDirectoryRef: typed.ActorRef[WsExternalClientDirectoryActor.Message]
)(implicit mat: Materializer)
    extends ApiRoute
    with ProtectDirective
    with HasStatusBarrier
    with AuthRoute
    with ScorexLogging {

  override lazy val route: Route = pathPrefix("matcher" / "settings" / "rates") {
    getAssetRates ~ matcherStatusBarrier(upsertAssetRate ~ deleteAssetRate)
  }

  @Path("/settings/rates#getAssetRates")
  @ApiOperation(
    value = "Asset rates",
    notes = "Get current rates of assets (price of 1 Waves in the specified asset), returns Map[Base58 encoded Asset ID, Double]",
    httpMethod = "GET",
    tags = Array("rates"),
    response = classOf[HttpRates]
  )
  def getAssetRates: Route = (pathEndOrSingleSlash & get)(withMetricsAndTraces("getAssetRates")(complete(rateCache.getAllRates.toJson)))

  @Path("/settings/rates/{assetId}#upsertAssetRate")
  @ApiOperation(
    value = "Add or update rate for the specified asset. Requires API Key",
    httpMethod = "PUT",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("rates"),
    response = classOf[HttpMessage]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "assetId", value = "Asset for which rate is added or updated", dataType = "string", paramType = "path"),
      new ApiImplicitParam(
        name = "rate",
        value = "Rate associated with the specified asset",
        dataType = "double",
        paramType = "body",
        required = true
      )
    )
  )
  def upsertAssetRate: Route =
    (path(AssetPM) & put) { assetOrError =>
      (withMetricsAndTraces("upsertAssetRate") & protect & withAuth) {
        entity(as[Double]) { rate =>
          if (rate.isInfinite || rate <= 0)
            complete(RateError(error.InvalidAssetRate))
          else
            withAsset(assetPairBuilder, assetOrError) { asset =>
              complete(
                if (asset == Waves) RateError(error.WavesImmutableRate)
                else {
                  val assetStr = asset.toString
                  val response = rateCache.upsertRate(asset, rate) match {
                    case None => SimpleResponse(StatusCodes.Created, s"The rate $rate for the asset $assetStr added")
                    case Some(pv) =>
                      SimpleResponse(StatusCodes.OK, s"The rate for the asset $assetStr updated, old value = $pv, new value = $rate")
                  }
                  externalClientDirectoryRef ! WsExternalClientDirectoryActor.Command.BroadcastRatesUpdates(Map(asset -> rate))
                  response
                }
              )
            }
        }
      }
    }

  @Path("/settings/rates/{assetId}#deleteAssetRate")
  @ApiOperation(
    value = "Delete rate for the specified asset. Requires API Key",
    httpMethod = "DELETE",
    authorizations = Array(new Authorization(SwaggerDocService.apiKeyDefinitionName)),
    tags = Array("rates"),
    response = classOf[HttpMessage]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "assetId", value = "Asset for which rate is deleted", dataType = "string", paramType = "path")
    )
  )
  def deleteAssetRate: Route =
    (path(AssetPM) & delete) { assetOrError =>
      (withMetricsAndTraces("deleteAssetRate") & protect & withAuth) {
        withAsset(assetPairBuilder, assetOrError) { asset =>
          complete(
            if (asset == Waves) RateError(error.WavesImmutableRate)
            else {
              val assetStr = asset.toString
              val response = rateCache.deleteRate(asset) match {
                case None => RateError(error.RateNotFound(asset), StatusCodes.NotFound)
                case Some(pv) => SimpleResponse(StatusCodes.OK, s"The rate for the asset $assetStr deleted, old value = $pv")
              }
              externalClientDirectoryRef ! WsExternalClientDirectoryActor.Command.BroadcastRatesUpdates(Map(asset -> -1))
              response
            }
          )
        }
      }
    }

}
