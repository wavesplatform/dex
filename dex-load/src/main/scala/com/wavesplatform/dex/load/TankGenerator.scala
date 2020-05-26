package com.wavesplatform.dex.load

import com.typesafe.config.ConfigFactory
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.load.utils._
import com.wavesplatform.wavesj.matcher.Order
import com.wavesplatform.wavesj.{AssetPair, PrivateKeyAccount, Transactions}

import scala.collection.mutable
import scala.util.Random

object TankGenerator extends ScorexLogging {
  private def mkAccounts(seedPrefix: String, env: Environment, count: Int = 1000) = {
    (for { i <- 1 to count } yield { PrivateKeyAccount.fromSeed(s"$seedPrefix$i", 0, env.networkByte) }).toList
  }

  private def mkAssets(env: Environment, count: Int = 10): List[String] = {
    (for { _ <- 1 to count } yield {
      val tx =
        Transactions.makeIssueTx(
          env.issuer,
          env.networkByte,
          Random.nextInt(100000).toString,
          Random.nextInt(100000).toString,
          env.assetQuantity,
          8, //TODO: random from 2 to 16
          false,
          null,
          env.issueFee
        )
      log.info(tx.toString)
      env.node.send(tx)
      tx.getId.toString
    }).toList
  }

  private def mkAssetPairs(assets: List[String], count: Int = 10): List[AssetPair] = {
    val pairs: mutable.HashSet[AssetPair] = mutable.HashSet()

    while (pairs.size <= count) pairs += new AssetPair(assets(Random.nextInt(assets.size - 1)), assets(assets.size - 1))
    pairs.toList
  }

  private def distributeAssets(accounts: List[PrivateKeyAccount], assets: List[String], env: Environment): Unit = {
    accounts
      .flatMap { acc =>
        Transactions.makeTransferTx(env.issuer, acc.getAddress, 10000000000L, "WAVES", 300000, "WAVES", "") :: assets.map { ass =>
          Transactions.makeTransferTx(env.issuer, acc.getAddress, 100000, ass, 300000, "WAVES", "")
        }
      }
      .flatMap(env.node.send(_))
  }

  private def mkOrders(env: Environment, accounts: List[PrivateKeyAccount], pairs: List[AssetPair]): List[Order] = {
    val orders: mutable.HashSet[Order] = mutable.HashSet()

    orders += Transactions.makeOrder(
      PrivateKeyAccount.fromSeed("sds", 0, env.networkByte),
      env.matcherPublicKey,
      com.wavesplatform.wavesj.matcher.Order.Type.BUY,
      pairs(0),
      1002332,
      100500,
      System.currentTimeMillis + 1005000,
      300000
    )

    orders.toList
  }

  private def mkPlaces(seedPrefix: String, env: Environment): Unit = {
    log.info("mkPlaces")
    val accounts = mkAccounts(seedPrefix, env, 100)
    val assets   = mkAssets(env)

    waitForHeightArise(env)

    val pairs = mkAssetPairs(assets)

    distributeAssets(accounts, assets, env)
    waitForHeightArise(env)

    val orders = mkOrders(env, accounts, pairs)

    print(postRequest(env, orders(0), "/orderbook/place", "place"))
  }

  private def mkCancels(seedPrefix: String, env: Environment): Unit = {
    log.info("mkCancels")
  }

  private def mkMatching(seedPrefix: String, env: Environment): Unit = {
    log.info("mkMatching")
  }

  private def mkOrderHistory(seedPrefix: String, env: Environment): Unit = {
    log.info("Wrong number")
  }

  private def mkAllTypes(): Unit = {
    log.info("mkAllTypes")
  }

  def mkRequests(seedPrefix: String, environment: String, task: Int = 1): Unit = {
    val env = new Environment(ConfigFactory.parseResources(environment))

    task match {
      case 1 => mkPlaces(seedPrefix, env)
      case 2 => mkCancels(seedPrefix, env)
      case 3 => mkMatching(seedPrefix, env)
      case 4 => mkOrderHistory(seedPrefix, env)
      case 5 => mkAllTypes
      case _ =>
        log.info("Wrong number")
    }
  }
}
