package com.wavesplatform.dex.api.http.routes.v0

import akka.actor.ActorRef
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.util.Timeout
import com.wavesplatform.dex.actors.address.AddressActor
import com.wavesplatform.dex.api.http.HasStatusBarrier
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives._
import com.wavesplatform.dex.api.http.directives.ProtectDirective
import com.wavesplatform.dex.api.http.entities._
import com.wavesplatform.dex.api.routes.PathMatchers.{AddressPM, AssetPairPM, PublicKeyPM}
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.domain.account.PublicKey
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.model.AssetPairBuilder
import com.wavesplatform.dex.settings.MatcherSettings
import io.swagger.annotations.{Api, _}

import javax.ws.rs.Path
import scala.concurrent.ExecutionContext

@Path("/matcher")
@Api()
class BalancesRoute(
  assetPairBuilder: AssetPairBuilder,
  addressActor: ActorRef,
  override val matcherStatus: () => MatcherStatus,
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

  override lazy val route: Route =
    pathPrefix("matcher") {
      pathPrefix("orderbook") {
        matcherStatusBarrier {
          getTradableBalanceByAssetPairAndAddress
        }
      } ~ pathPrefix("balance")(getReservedBalanceByPK)
    }

  @Path("/orderbook/{amountAsset}/{priceAsset}/tradableBalance/{address}#getTradableBalanceByAssetPairAndAddress")
  @ApiOperation(
    value = "Tradable Balance for Asset Pair",
    notes = "Get Tradable Balance for the given Asset Pair, returns Map[Base58 encoded Asset ID, Long]",
    httpMethod = "GET",
    tags = Array("balances"),
    response = classOf[HttpBalance]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "amountAsset", value = "Amount Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "priceAsset", value = "Price Asset ID in Pair, or 'WAVES'", dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "address", value = "Account Address", required = true, dataType = "string", paramType = "path")
    )
  )
  def getTradableBalanceByAssetPairAndAddress: Route =
    (path(AssetPairPM / "tradableBalance" / AddressPM) & get) { (pairOrError, addressOrError) =>
      (withMetricsAndTraces("getTradableBalanceByAssetPairAndAddress") & protect) {
        withAddress(addressOrError) { address =>
          withAssetPair(assetPairBuilder, pairOrError, redirectToInverse = true, s"/tradableBalance/$address") { pair =>
            complete {
              askMapAddressActor[AddressActor.Reply.GetBalance](addressActor, address, AddressActor.Query.GetTradableBalance(pair.assets))(
                _.balance.toJson
              )
            }
          }
        }
      }
    }

  @Path("/balance/reserved/{publicKey}#getReservedBalanceByPK")
  @ApiOperation(
    value = "Reserved Balance. Requires API Key if signature isn't provided",
    notes = "Get non-zero balance of open orders, returns Map[Base58 encoded Asset ID, Long]",
    httpMethod = "GET",
    tags = Array("balances"),
    response = classOf[HttpBalance]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "publicKey", value = "Public Key", required = true, dataType = "string", paramType = "path"),
      new ApiImplicitParam(name = "Timestamp", value = "Timestamp", required = true, dataType = "integer", paramType = "header"),
      new ApiImplicitParam(
        name = "Signature",
        value = "Base58 encoded Curve25519.sign(senderPrivateKey, concat(bytesOf(publicKey), bigEndianBytes(Timestamp)))",
        required = true,
        dataType = "string",
        paramType = "header"
      )
    )
  )
  def getReservedBalanceByPK: Route = (path("reserved" / PublicKeyPM) & get) { publicKeyOrError =>
    (withMetricsAndTraces("getReservedBalanceByPK") & protect) {
      withPublicKey(publicKeyOrError) { publicKey =>
        (signedGet(publicKey).tmap(_ => Option.empty[PublicKey]) | (withAuth & withUserPublicKeyOpt)) {
          case Some(upk) if upk != publicKey => invalidUserPublicKey
          case _ =>
            complete {
              askMapAddressActor[AddressActor.Reply.GetBalance](addressActor, publicKey, AddressActor.Query.GetReservedBalance)(_.balance.toJson)
            }
        }
      }
    }
  }

}
