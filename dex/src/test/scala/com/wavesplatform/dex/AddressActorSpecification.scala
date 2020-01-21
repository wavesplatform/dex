package com.wavesplatform.dex

import java.util.concurrent.atomic.AtomicReference

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import cats.kernel.Monoid
import com.wavesplatform.dex.AddressActor.Command.{CancelNotEnoughCoinsOrders, PlaceOrder}
import com.wavesplatform.dex.db.EmptyOrderDB
import com.wavesplatform.dex.domain.account.{Address, KeyPair, PublicKey}
import com.wavesplatform.dex.domain.asset.Asset.{IssuedAsset, Waves}
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.order.{Order, OrderType, OrderV1}
import com.wavesplatform.dex.domain.state.{LeaseBalance, Portfolio}
import com.wavesplatform.dex.error.ErrorFormatterContext
import com.wavesplatform.dex.model.Events.OrderAdded
import com.wavesplatform.dex.model.{LimitOrder, OrderBook}
import com.wavesplatform.dex.queue.{QueueEvent, QueueEventWithMeta}
import com.wavesplatform.dex.time.NTPTime
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future

class AddressActorSpecification
    extends TestKit(ActorSystem("AddressActorSpecification"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ImplicitSender
    with NTPTime {

  private implicit val efc: ErrorFormatterContext = (_: Asset) => 8

  private val assetId    = ByteStr("asset".getBytes("utf-8"))
  private val matcherFee = 30000L

  private val sellTokenOrder1 = OrderV1(
    sender = privateKey("test"),
    matcher = PublicKey("matcher".getBytes("utf-8")),
    pair = AssetPair(Waves, IssuedAsset(assetId)),
    orderType = OrderType.BUY,
    price = 100000000L,
    amount = 100L,
    timestamp = 1L,
    expiration = 1000L,
    matcherFee = matcherFee
  )

  private val sellToken1Portfolio = requiredPortfolio(sellTokenOrder1)

  private val sellTokenOrder2 = OrderV1(
    sender = privateKey("test"),
    matcher = PublicKey("matcher".getBytes("utf-8")),
    pair = AssetPair(Waves, IssuedAsset(assetId)),
    orderType = OrderType.BUY,
    price = 100000000L,
    amount = 100L,
    timestamp = 2L,
    expiration = 1000L,
    matcherFee = matcherFee
  )

  private val sellToken2Portfolio = requiredPortfolio(sellTokenOrder2)

  private val sellWavesOrder = OrderV1(
    sender = privateKey("test"),
    matcher = PublicKey("matcher".getBytes("utf-8")),
    pair = AssetPair(Waves, IssuedAsset(assetId)),
    orderType = OrderType.SELL,
    price = 100000000L,
    amount = 100L,
    timestamp = 3L,
    expiration = 1000L,
    matcherFee = matcherFee
  )

  private val sellWavesPortfolio = requiredPortfolio(sellWavesOrder)

  "AddressActorSpecification" should {
    "cancel orders" when {
      "asset balance changed" in test { (ref, eventsProbe, updatePortfolio) =>
        val initPortfolio = sellToken1Portfolio
        updatePortfolio(initPortfolio, false)

        ref ! PlaceLimitOrder(sellTokenOrder1)
        eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellTokenOrder1)))
        ref ! OrderAdded(LimitOrder(sellTokenOrder1), System.currentTimeMillis)

        updatePortfolio(initPortfolio.copy(assets = Map.empty), true)
        eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder1.assetPair, sellTokenOrder1.id()))
      }

      "waves balance changed" when {
        "there are waves for fee" in wavesBalanceTest(restWaves = matcherFee)
        "there are no waves at all" in wavesBalanceTest(restWaves = 0L)

        def wavesBalanceTest(restWaves: Long): Unit = test { (ref, eventsProbe, updatePortfolio) =>
          val initPortfolio = sellWavesPortfolio
          updatePortfolio(initPortfolio, false)

          ref ! PlaceLimitOrder(sellWavesOrder)
          eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellWavesOrder)))

          updatePortfolio(initPortfolio.copy(balance = restWaves), true)
          eventsProbe.expectMsg(QueueEvent.Canceled(sellWavesOrder.assetPair, sellWavesOrder.id()))
        }
      }

      "waves were leased" when {
        "there are waves for fee" in leaseTest(_ => matcherFee)
        "there are no waves at all" in leaseTest(_.spendableBalance)

        def leaseTest(leasedWaves: Portfolio => Long): Unit = test { (ref, eventsProbe, updatePortfolio) =>
          val initPortfolio = sellWavesPortfolio
          updatePortfolio(initPortfolio, false)

          ref ! PlaceLimitOrder(sellWavesOrder)
          eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellWavesOrder)))

          updatePortfolio(initPortfolio.copy(lease = LeaseBalance(0, leasedWaves(initPortfolio))), true)
          eventsProbe.expectMsg(QueueEvent.Canceled(sellWavesOrder.assetPair, sellWavesOrder.id()))
        }
      }
    }

    "track canceled orders and don't cancel more on same BalanceUpdated message" in test { (ref, eventsProbe, updatePortfolio) =>
      val initPortfolio = Monoid.combine(sellToken1Portfolio, sellToken2Portfolio)
      updatePortfolio(initPortfolio, false)

      ref ! PlaceLimitOrder(sellTokenOrder1)
      eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellTokenOrder1)))
      ref ! OrderAdded(LimitOrder(sellTokenOrder1), System.currentTimeMillis)

      ref ! PlaceLimitOrder(sellTokenOrder2)
      eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellTokenOrder2)))
      ref ! OrderAdded(LimitOrder(sellTokenOrder2), System.currentTimeMillis)

      updatePortfolio(sellToken1Portfolio, true)
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder2.assetPair, sellTokenOrder2.id()))

      updatePortfolio(sellToken1Portfolio, true) // same event
      eventsProbe.expectNoMessage()
    }

    "cancel multiple orders" in test { (ref, eventsProbe, updatePortfolio) =>
      val initPortfolio = Monoid.combineAll(Seq(sellToken1Portfolio, sellToken2Portfolio, sellWavesPortfolio))
      updatePortfolio(initPortfolio, false)

      ref ! PlaceLimitOrder(sellTokenOrder1)
      eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellTokenOrder1)))
      ref ! OrderAdded(LimitOrder(sellTokenOrder1), System.currentTimeMillis)

      ref ! PlaceLimitOrder(sellTokenOrder2)
      eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellTokenOrder2)))
      ref ! OrderAdded(LimitOrder(sellTokenOrder2), System.currentTimeMillis)

      updatePortfolio(sellWavesPortfolio, true)
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder1.assetPair, sellTokenOrder1.id()))
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder2.assetPair, sellTokenOrder2.id()))
    }

    "cancel only orders, those aren't fit" in test { (ref, eventsProbe, updatePortfolio) =>
      val initPortfolio = Monoid.combineAll(Seq(sellToken1Portfolio, sellToken2Portfolio, sellWavesPortfolio))
      updatePortfolio(initPortfolio, false)

      ref ! PlaceLimitOrder(sellTokenOrder1)
      eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellTokenOrder1)))
      ref ! OrderAdded(LimitOrder(sellTokenOrder1), System.currentTimeMillis)

      ref ! PlaceLimitOrder(sellWavesOrder)
      eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellWavesOrder)))
      ref ! OrderAdded(LimitOrder(sellWavesOrder), System.currentTimeMillis)

      ref ! PlaceLimitOrder(sellTokenOrder2)
      eventsProbe.expectMsg(QueueEvent.Placed(LimitOrder(sellTokenOrder2)))
      ref ! OrderAdded(LimitOrder(sellTokenOrder2), System.currentTimeMillis)

      updatePortfolio(sellWavesPortfolio, true)
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder1.assetPair, sellTokenOrder1.id()))
      eventsProbe.expectMsg(QueueEvent.Canceled(sellTokenOrder2.assetPair, sellTokenOrder2.id()))
    }

    "schedule expired order cancellation" in {
      pending
    }
  }

  /**
    * (updatedPortfolio: Portfolio, sendBalanceChanged: Boolean) => Unit
    */
  private def test(f: (ActorRef, TestProbe, (Portfolio, Boolean) => Unit) => Unit): Unit = {

    val eventsProbe      = TestProbe()
    val currentPortfolio = new AtomicReference[Portfolio]()
    val address          = addr("test")

    val addressActor =
      system.actorOf(
        Props(
          new AddressActor(
            address,
            x => Future.successful { currentPortfolio.get().spendableBalanceOf(x) },
            ntpTime,
            EmptyOrderDB,
            _ => Future.successful(false),
            event => {
              eventsProbe.ref ! event
              Future.successful { Some(QueueEventWithMeta(0, 0, event)) }
            },
            _ => OrderBook.AggregatedSnapshot(),
            false
          )
        )
      )
    f(
      addressActor,
      eventsProbe,
      (updatedPortfolio, notify) => {
        val prevPortfolio = currentPortfolio.getAndSet(updatedPortfolio)
        if (notify)
          addressActor !
            CancelNotEnoughCoinsOrders {
              prevPortfolio
                .changedAssetIds(updatedPortfolio)
                .map(asset => asset -> updatedPortfolio.spendableBalanceOf(asset))
                .toMap
                .withDefaultValue(0)
            }
      }
    )

    addressActor ! PoisonPill
  }

  private def requiredPortfolio(order: Order): Portfolio = {
    val b = LimitOrder(order).requiredBalance
    Portfolio(b.getOrElse(Waves, 0L), LeaseBalance.empty, b.collect { case (id @ IssuedAsset(_), v) => id -> v })
  }

  private def addr(seed: String): Address       = privateKey(seed).toAddress
  private def privateKey(seed: String): KeyPair = KeyPair(seed.getBytes("utf-8"))

  private def PlaceLimitOrder(o: Order): AddressActor.Command.PlaceOrder = PlaceOrder(o, isMarket = false)

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
