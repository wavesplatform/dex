package com.wavesplatform.dex.api.http.routes.v0

import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.wavesplatform.dex._
import com.wavesplatform.dex.api.http.HasStatusBarrier
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives._
import com.wavesplatform.dex.api.http.directives.ProtectDirective
import com.wavesplatform.dex.api.http.entities._
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.caches.RateCache
import com.wavesplatform.dex.domain.account.PublicKey
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.settings.{MatcherSettings, OrderFeeSettings}
import io.swagger.annotations._

import javax.ws.rs.Path
import scala.concurrent.{ExecutionContext, Future}

@Path("/matcher")
@Api()
class InfoRoute(
  matcherPublicKey: PublicKey,
  matcherSettings: MatcherSettings,
  override val matcherStatus: () => MatcherStatus,
  matcherAccountFee: Long,
  override val apiKeyHash: Option[Array[Byte]],
  rateCache: RateCache,
  validatedAllowedOrderVersions: () => Future[Set[Byte]],
  getActualOrderFeeSettings: () => OrderFeeSettings
)(implicit mat: Materializer)
    extends ApiRoute
    with ProtectDirective
    with HasStatusBarrier
    with AuthRoute
    with ScorexLogging {

  implicit private val executionContext: ExecutionContext = mat.executionContext

  override lazy val route: Route =
    pathPrefix("matcher")(pathPrefix("settings")(getMatcherPublicSettings) ~ getMatcherPKInBase58)

  @Path("/#getMatcherPKInBase58")
  @ApiOperation(
    value = "Matcher Public Key",
    notes = "Get Matcher Public Key in Base58",
    httpMethod = "GET",
    tags = Array("info"),
    response = classOf[String]
  )
  def getMatcherPKInBase58: Route =
    (pathEndOrSingleSlash & get)((withMetricsAndTraces("getMatcherPKInBase58") & protect)(
      complete(matcherPublicKey.toJson)
    ))

  @Path("/settings#getMatcherPublicSettings")
  @ApiOperation(
    value = "Matcher Settings",
    notes = "Get Matcher Public Settings",
    httpMethod = "GET",
    tags = Array("info"),
    response = classOf[HttpMatcherPublicSettings]
  )
  def getMatcherPublicSettings: Route =
    (pathEndOrSingleSlash & get) {
      withMetricsAndTraces("getMatcherPublicSettings") {
        complete(
          validatedAllowedOrderVersions() map { allowedOrderVersions =>
            SimpleResponse(
              HttpMatcherPublicSettings(
                matcherPublicKey = matcherPublicKey,
                matcherVersion = Version.VersionString,
                priceAssets = matcherSettings.priceAssets,
                orderFee = HttpOrderFeeMode.fromSettings(
                  settings = getActualOrderFeeSettings(),
                  matcherAccountFee = matcherAccountFee,
                  allRates = rateCache.getAllRates
                ),
                orderVersions = allowedOrderVersions.toSeq.sorted,
                networkByte = matcherSettings.addressSchemeCharacter.toInt
              )
            )
          }
        )
      }
    }

}
