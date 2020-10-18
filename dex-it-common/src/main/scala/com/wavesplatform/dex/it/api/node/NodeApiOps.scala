//package com.wavesplatform.dex.it.api.node
//
//import java.net.InetSocketAddress
//
//import cats.Functor
//import cats.syntax.functor._
//import com.typesafe.config.Config
//import com.wavesplatform.dex.domain.account.Address
//import com.wavesplatform.dex.domain.asset.Asset
//import com.wavesplatform.dex.domain.asset.Asset.{IssuedAsset, Waves}
//import com.wavesplatform.dex.it.api.responses.node.{ActivationStatusResponse, AssetBalanceResponse, ConnectedPeersResponse, WavesBalanceResponse}
//import com.wavesplatform.dex.it.fp.CanExtract
//import im.mak.waves.transactions.Transaction
//import im.mak.waves.transactions.common.Id
//
//object NodeApiOps {
//
//  implicit final class ExplicitGetNodeApiOps[F[_]: Functor](val self: NodeApi[F])(implicit E: CanExtract[F]) {
//
//    import E.{extract => explicitGet}
//
//    def wavesBalance(address: Address): F[WavesBalanceResponse] = explicitGet(self.tryWavesBalance(address))
//    def assetBalance(address: Address, asset: IssuedAsset): F[AssetBalanceResponse] = explicitGet(self.tryAssetBalance(address, asset))
//
//    def balance(address: Address, asset: Asset): F[Long] = asset match {
//      case Waves => wavesBalance(address).map(_.balance)
//      case x: IssuedAsset => assetBalance(address, x).map(_.balance)
//    }
//
//    def connect(toNode: InetSocketAddress): F[Unit] = explicitGet(self.tryConnect(toNode))
//
//    def connectedPeers: F[ConnectedPeersResponse] = explicitGet(self.tryConnectedPeers)
//
//    def broadcast(tx: Transaction): F[Unit] = explicitGet(self.tryBroadcast(tx))
//
//    def transactionInfo(id: Id): F[Option[Transaction]] = self.tryTransactionInfo(id).map {
//      case Right(r) => Some(r)
//      case Left(e) =>
//        if (e.error == 311) None // node's ApiError.TransactionDoesNotExist.id
//        else throw new RuntimeException(s"Unexpected error: $e")
//    }
//
//    def currentHeight: F[Int] = explicitGet(self.tryCurrentHeight)
//
//    def activationStatus: F[ActivationStatusResponse] = explicitGet(self.tryActivationStatus)
//    def config: F[Config] = explicitGet(self.tryConfig)
//  }
//
//}
