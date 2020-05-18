package com.wavesplatform.it.api

import com.wavesplatform.dex.domain.account.KeyPair
import com.wavesplatform.dex.domain.asset.{Asset, AssetPair}
import com.wavesplatform.dex.it.api.responses.dex.{MarketStatusResponse, OrderBookHistoryItem, OrderBookResponse, OrderStatusResponse}
import com.wavesplatform.dex.queue.QueueEventWithMeta

case class MatcherState(offset: QueueEventWithMeta.Offset,
                        snapshots: Map[AssetPair, QueueEventWithMeta.Offset],
                        orderBooks: Map[AssetPair, (OrderBookResponse, MarketStatusResponse)],
                        orderStatuses: Map[String, OrderStatusResponse],
                        orderTransactionIds: Map[String, Seq[String]],
                        reservedBalances: Map[KeyPair, Map[Asset, Long]],
                        orderHistory: Map[KeyPair, Map[AssetPair, Seq[OrderBookHistoryItem]]]) {
  override def toString: String =
    s"""MatcherState(
       |  offset=$offset,
       |  snapshots=$snapshots,
       |  orderBooks=$orderBooks,
       |  orderStatuses=$orderStatuses,
       |  orderTransactionIds=$orderTransactionIds,
       |  reservedBalances=$reservedBalances,
       |  orderHistory=$orderHistory
       |)""".stripMargin
}
