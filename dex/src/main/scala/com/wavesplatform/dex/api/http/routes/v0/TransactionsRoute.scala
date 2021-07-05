package com.wavesplatform.dex.api.http.routes.v0

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server._
import akka.stream.Materializer
import com.wavesplatform.dex.api.http.directives.HttpKamonDirectives._
import com.wavesplatform.dex.api.http.directives.ProtectDirective
import com.wavesplatform.dex.api.http.{HasStatusBarrier, _}
import com.wavesplatform.dex.api.routes.PathMatchers.OrderPM
import com.wavesplatform.dex.api.routes.{ApiRoute, AuthRoute}
import com.wavesplatform.dex.app.MatcherStatus
import com.wavesplatform.dex.db.OrderDb
import com.wavesplatform.dex.domain.transaction.ExchangeTransactionV2
import com.wavesplatform.dex.domain.utils.ScorexLogging
import io.swagger.annotations._
import play.api.libs.json._

import javax.ws.rs.Path
import scala.concurrent.{ExecutionContext, Future}

@Path("/matcher")
@Api()
class TransactionsRoute(
  override val matcherStatus: () => MatcherStatus,
  orderDb: OrderDb[Future],
  override val apiKeyHash: Option[Array[Byte]]
)(implicit mat: Materializer)
    extends ApiRoute
    with ProtectDirective
    with HasStatusBarrier
    with AuthRoute
    with ScorexLogging {

  implicit private val executionContext: ExecutionContext = mat.executionContext

  override lazy val route: Route = pathPrefix("matcher" / "transactions")(getTransactionsByOrderId)

  @Path("/transactions/{orderId}#getTransactionsByOrderId")
  @ApiOperation(
    value = "Get Exchange Transactions by Order",
    notes = "Get all exchange transactions created by DEX on execution of the given order",
    httpMethod = "GET",
    tags = Array("transactions"),
    response = classOf[Array[ExchangeTransactionV2]]
  )
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "orderId", value = "Order ID", dataType = "string", paramType = "path")
    )
  )
  def getTransactionsByOrderId: Route =
    (path(OrderPM) & get) { orderIdOrError =>
      (withMetricsAndTraces("getTransactionsByOrderId") & protect) {
        withOrderId(orderIdOrError) { orderId =>
          complete {
            orderDb.transactionsByOrder(orderId).map(x => ToResponseMarshallable(Json.toJson(x))).recover {
              case th =>
                log.error("error while retrieving order transactions", th)
                ToResponseMarshallable(entities.InternalError)
            }
          }
        }
      }
    }

}
