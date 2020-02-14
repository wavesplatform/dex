package com.wavesplatform.dex.model

import java.nio.charset.StandardCharsets

import cats.instances.long.catsKernelStdGroupForLong
import cats.syntax.group._
import cats.kernel.{Group, Monoid}
import com.wavesplatform.dex.domain.account.{KeyPair, PublicKey}
import com.wavesplatform.dex.domain.asset.Asset.{IssuedAsset, Waves}
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.order.{Order, OrderType}
import com.wavesplatform.dex.fp.MapImplicits.group
import com.wavesplatform.dex.{MatcherSpecBase, NoShrink}
import org.scalacheck.Gen
import org.scalacheck.Gen.Choose
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.immutable.NumericRange

class OrderBookSpec extends AnyFreeSpecLike with MatcherSpecBase with Matchers with ScalaCheckPropertyChecks with NoShrink {

  private val matcher = KeyPair(ByteStr("matcher".getBytes(StandardCharsets.UTF_8)))
  private val assetPair = AssetPair(
    amountAsset = IssuedAsset(ByteStr("amount-asset".getBytes(StandardCharsets.UTF_8))),
    priceAsset = Waves
  )
  private val thirdAsset = IssuedAsset(ByteStr("third-asset".getBytes(StandardCharsets.UTF_8)))
  private val ts         = 1000L
  private val expiration = ts + 60000L

  private val assetGen = Gen.oneOf[Asset](assetPair.amountAsset, assetPair.priceAsset, thirdAsset)

  private val clients     = (1 to 3).map(clientId => KeyPair(ByteStr(s"client-$clientId".getBytes(StandardCharsets.UTF_8))))
  protected val clientGen = Gen.oneOf(clients)

  // TODO migrate to long ranges in 2.13
  private val askPricesMin = 1000L * Order.PriceConstant
  private val askPricesMax = 2000L * Order.PriceConstant
  private val askPricesGen = Gen.choose(askPricesMin, askPricesMax)

  private val bidPricesMin = 1L * Order.PriceConstant
  private val bidPricesMax = 999L * Order.PriceConstant
  private val bidPricesGen = Gen.choose(bidPricesMin, bidPricesMax)

  private val allPricesGen = Gen.choose(bidPricesMin, askPricesMax)

  private val askLimitOrderGen: Gen[LimitOrder] = limitOrderGen(orderGen(askPricesGen, OrderType.SELL))
  private val bidLimitOrderGen: Gen[LimitOrder] = limitOrderGen(orderGen(bidPricesGen, OrderType.BUY))
  private val limitOrderGen: Gen[LimitOrder] = orderTypeGenerator.flatMap { x =>
    limitOrderGen(orderGen(allPricesGen, x))
  }

  private val invariantTestGen = for {
    askOrders <- Gen.listOfN(3, askLimitOrderGen)
    bidOrders <- Gen.listOfN(3, bidLimitOrderGen)
    newOrder  <- limitOrderGen //limitOrderGen(orderGen(askOrders.map(_.order.price).toVector.min, OrderType.BUY)) // TODO limitOrderGen
  } yield (askOrders, bidOrders, newOrder)

  "coins invariant" in forAll(invariantTestGen) {
    case (askOrders, bidOrders, newOrder) =>
      val ob = OrderBook(
        OrderBookSnapshot(
          bidOrders.groupBy(_.order.price),
          askOrders.groupBy(_.order.price),
          lastTrade = None
        ))

      val obBefore    = format(ob)
      val coinsBefore = countCoins(ob) |+| newOrder.requiredBalance // do not change
      val events      = ob.add(newOrder, ts, getMakerTakerFee = (o1, o2) => (o1.matcherFee, o2.matcherFee))

      val eventsDiff = Monoid.combineAll(events.map {
        case evt: Events.OrderExecuted =>
          // remaining

          val price            = evt.counter.price
          val submittedSpent   = Map(evt.submitted.spentAsset -> evt.submitted.order.getSpendAmount(evt.executedAmount, price).right.get)
          val submittedReceive = Map(evt.submitted.rcvAsset -> evt.submitted.order.getReceiveAmount(evt.executedAmount, price).right.get)
          val submittedSpentFee =
            Map(evt.submitted.feeAsset -> AcceptedOrder.partialFee(evt.submitted.order.matcherFee, evt.submitted.order.amount, evt.executedAmount))

          val submittedCompensation = Map(
            evt.submitted.spentAsset -> (
              evt.submitted.order.getSpendAmount(evt.executedAmount, evt.submitted.order.price).right.get -
                evt.submitted.order.getSpendAmount(evt.executedAmount, price).right.get
              ))

          val counterSpent   = Map(evt.counter.spentAsset -> evt.counter.order.getSpendAmount(evt.executedAmount, price).right.get)
          val counterReceive = Map(evt.counter.rcvAsset   -> evt.counter.order.getReceiveAmount(evt.executedAmount, price).right.get)
          val counterSpentFee =
            Map(evt.counter.feeAsset -> AcceptedOrder.partialFee(evt.counter.order.matcherFee, evt.counter.order.amount, evt.executedAmount))

          submittedSpent should matchTo(counterReceive)
          counterSpent should matchTo(submittedReceive)

          println(s"""
submittedReceive:
${submittedReceive.mkString("\n")}

counterReceive:
${counterReceive.mkString("\n")}

evt.submitted.order.spentAmount:
${evt.submitted.spentAsset} -> ${evt.submitted.spentAmount}

evt.submitted.spentAmount :
${evt.submitted.spentAsset} -> ${evt.submitted.spentAmount}
""")

          Monoid.combineAll(
            Seq(
              submittedReceive,
              submittedSpentFee,
              submittedCompensation,
              counterReceive,
              counterSpentFee
            ))

        case evt: Events.OrderCanceled => evt.acceptedOrder.requiredBalance
        case evt: Events.OrderAdded    =>
          // Group.inverse(evt.order.requiredBalance)
          Map.empty[Asset, Long] //
      })

      // TODO find order in order book and compensate
      //val updatedOrder = ob.allOrders.map(_._2).find(_.order.id() == newOrder.order.id()).get

      val coinsAfter = countCoins(ob) |+| eventsDiff // |+| (newOrder.requiredBalance |-| updatedOrder.requiredBalance)

      val diff = coinsAfter |-| coinsBefore
      val clue =
        s"""
Pair:
$assetPair

Order:
${format(newOrder)}

OrderBook before:
$obBefore

OrderBook after:
${format(ob)}

Events:
${events.mkString("\n")}

Events diff:
${eventsDiff.mkString("\n")}

Diff:
${diff.mkString("\n")}
"""

      withClue(clue) {
        coinsBefore should matchTo(coinsAfter)
      }
  }

  private def limitOrderGen(orderGen: Gen[Order]): Gen[LimitOrder] =
    for {
      order      <- orderGen
      restAmount <- Gen.const(order.amount) // TODO Gen.choose(minAmount(order.price), order.amount)
    } yield {
      val restFee = AcceptedOrder.partialFee(order.matcherFee, order.amount, restAmount)
      order.orderType match {
        case OrderType.SELL => SellLimitOrder(restAmount, restFee, order)
        case OrderType.BUY  => BuyLimitOrder(restAmount, restFee, order)
      }
    }

  /**
    * @param pricesGen Should be multiplied by Order.PriceConstant
    */
  private def orderGen(pricesGen: Gen[Long], side: OrderType): Gen[Order] =
    for {
      owner    <- clientGen
      feeAsset <- Gen.const(thirdAsset) // TODO assetGen
      price    <- pricesGen
      amount <- {
        // The rule based on getReceiveAmount (for SELL orders) or getSpendAmount (for BUY orders)
        // In both cases we get same condition (20 here to escape cases when sum > Long.MaxValue):
        // amount: 1 <= amount * price / PriceConstant <= Long.MaxValue / 20
        // TODO
        val maxValue = BigInt(Long.MaxValue / 20) * Order.PriceConstant / price
        Gen.chooseNum(minAmount(price), 100) //maxValue.min(Long.MaxValue / 20).toLong)
        //Gen.const(minAmount(price))
      }
      version <- if (feeAsset == Waves) Gen.choose[Byte](1, 3) else Gen.const(3: Byte)
    } yield
      Order(
        sender = owner,
        matcher = matcher,
        pair = assetPair,
        orderType = side,
        amount = amount,
        price = price,
        timestamp = ts,
        expiration = expiration,
        matcherFee = matcherFee, // TODO
        version = version,
        feeAsset = feeAsset
      )

  private def countCoins(ob: OrderBook): Map[Asset, Long] = Monoid.combineAll((ob.getAsks.values ++ ob.getBids.values).flatten.map(_.requiredBalance))
  private def clientsPortfolio(ob: OrderBook): Map[PublicKey, Map[Asset, Long]] = {
    ???
  }

  private def minAmount(price: Long): Long = math.max(1, Order.PriceConstant / price)

  private def formatSide(xs: Iterable[(Long, Level)]): String =
    xs.map { case (p, orders) => s"$p -> ${orders.map(format).mkString(", ")}" }.mkString("\n")

  private def format(x: OrderBook): String = s"""
Asks (rcv=${assetPair.priceAsset}, spt=${assetPair.amountAsset}):
${formatSide(x.getAsks)}

Bids (rcv=${assetPair.amountAsset}, spt=${assetPair.priceAsset}):
${formatSide(x.getBids)}"""

  private def format(x: LimitOrder): String =
    s"""LimitOrder(a=${x.amount}, f=${x.fee}, ${format(x.order)}, rcv=${x.receiveAmount}, spt=${x.spentAmount})"""
  private def format(x: Order): String = s"""Order(${x.idStr()}, a=${x.amount}, p=${x.price}, f=${x.matcherFee} ${x.feeAsset})"""
}
