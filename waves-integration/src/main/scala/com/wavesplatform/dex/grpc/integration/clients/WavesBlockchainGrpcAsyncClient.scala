package com.wavesplatform.dex.grpc.integration.clients

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import com.wavesplatform.dex.domain.account.Address
import com.wavesplatform.dex.domain.asset.Asset
import com.wavesplatform.dex.domain.bytes.ByteStr
import com.wavesplatform.dex.domain.order.Order
import com.wavesplatform.dex.domain.transaction
import com.wavesplatform.dex.domain.utils.ScorexLogging
import com.wavesplatform.dex.grpc.integration.clients.WavesBlockchainClient.SpendableBalanceChanges
import com.wavesplatform.dex.grpc.integration.dto.BriefAssetDescription
import com.wavesplatform.dex.grpc.integration.effect.Implicits.NettyFutureOps
import com.wavesplatform.dex.grpc.integration.exceptions.{UnexpectedConnectionException, WavesNodeConnectionLostException}
import com.wavesplatform.dex.grpc.integration.protobuf.DexToPbConversions._
import com.wavesplatform.dex.grpc.integration.protobuf.PbToDexConversions._
import com.wavesplatform.dex.grpc.integration.services.RunScriptResponse.Result
import com.wavesplatform.dex.grpc.integration.services._
import io.grpc.stub.{ClientCallStreamObserver, ClientResponseObserver}
import io.grpc.{ManagedChannel, Status, StatusRuntimeException}
import io.netty.channel.EventLoopGroup
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject

import scala.concurrent.{ExecutionContext, Future}

/**
  * @param eventLoopGroup Here, because this class takes ownership
  * @param monixScheduler Is not an implicit, because it is ExecutionContext too
  */
class WavesBlockchainGrpcAsyncClient(eventLoopGroup: EventLoopGroup, channel: ManagedChannel, monixScheduler: Scheduler)(
    implicit grpcExecutionContext: ExecutionContext)
    extends WavesBlockchainClient[Future]
    with ScorexLogging {

  private def gRPCErrorsHandler(exception: Throwable): Throwable = exception match {
    case ex: io.grpc.StatusRuntimeException => WavesNodeConnectionLostException("Waves Node cannot be reached via gRPC", ex)
    case ex                                 => UnexpectedConnectionException("Unexpected connection error", ex)
  }

  private def handlingErrors[A](f: => Future[A]): Future[A] = { f transform (identity, gRPCErrorsHandler) }

  private val shuttingDown      = new AtomicBoolean(false)
  private val blockchainService = WavesBlockchainApiGrpc.stub(channel)

  private val spendableBalanceChangesSubject = ConcurrentSubject.publish[SpendableBalanceChanges](monixScheduler)

  private def toVanilla(record: BalanceChangesResponse.Record): (Address, Asset, Long) = {
    (record.address.toVanillaAddress, record.asset.toVanillaAsset, record.balance)
  }

  /**
    * Grouping batch records of spendable balances changes by addresses, e.g. converts
    * {{{ Seq[(Address, Asset, Balance)] to Map[Address, Map[Asset, Balance]] }}}
    */
  private def groupByAddress(balanceChangesResponse: BalanceChangesResponse): SpendableBalanceChanges = {
    balanceChangesResponse.batch
      .map { toVanilla }
      .groupBy { case (address, _, _) => address }
      .mapValues { _.map { case (_, asset, balance) => asset -> balance }.toMap.withDefaultValue(0) }
  }

  private val balanceChangesObserver = new BalanceChangesObserver

  /** Performs new gRPC call for receiving of the spendable balance changes stream */
  private def requestBalanceChanges(): Unit = blockchainService.getBalanceChanges(Empty(), balanceChangesObserver)

  private def parse(input: RunScriptResponse): RunScriptResult = input.result match {
    case Result.WrongInput(message)   => throw new IllegalArgumentException(message)
    case Result.Empty                 => RunScriptResult.Allowed
    case Result.ScriptError(message)  => RunScriptResult.ScriptError(message)
    case Result.UnexpectedResult(obj) => RunScriptResult.UnexpectedResult(obj)
    case Result.Exception(value)      => RunScriptResult.Exception(value.name, value.message)
    case _: Result.Denied             => RunScriptResult.Denied
  }

  /** Returns stream of the balance changes as a sequence of batches */
  override lazy val spendableBalanceChanges: Observable[SpendableBalanceChanges] = {
    requestBalanceChanges()
    spendableBalanceChangesSubject
  }

  override def spendableBalance(address: Address, asset: Asset): Future[Long] = handlingErrors {
    blockchainService
      .spendableAssetBalance { SpendableAssetBalanceRequest(address = address.toPB, assetId = asset.toPB) }
      .map(_.balance)
  }

  override def spendableBalances(address: Address, assets: Set[Asset]): Future[Map[Asset, Long]] = handlingErrors {
    blockchainService
      .spendableAssetsBalances {
        SpendableAssetsBalancesRequest(address = address.toPB, assets.map(a => SpendableAssetsBalancesRequest.Record(a.toPB)).toSeq)
      }
      .map(response => response.balances.map(record => record.assetId.toVanillaAsset -> record.balance).toMap)
  }

  override def allAssetsSpendableBalance(address: Address): Future[Map[Asset, Long]] = handlingErrors {
    blockchainService
      .allAssetsSpendableBalance { AddressRequest(address.toPB) }
      .map(response => response.balances.map(record => record.assetId.toVanillaAsset -> record.balance).toMap)
  }

  override def isFeatureActivated(id: Short): Future[Boolean] = handlingErrors {
    blockchainService.isFeatureActivated { IsFeatureActivatedRequest(id) }.map(_.isActivated)
  }

  override def assetDescription(asset: Asset.IssuedAsset): Future[Option[BriefAssetDescription]] = handlingErrors {
    blockchainService.assetDescription { AssetIdRequest(asset.toPB) }.map(_.maybeDescription.toVanilla)
  }

  override def hasScript(asset: Asset.IssuedAsset): Future[Boolean] = handlingErrors {
    blockchainService.hasAssetScript { AssetIdRequest(assetId = asset.toPB) }.map(_.has)
  }

  override def runScript(asset: Asset.IssuedAsset, input: transaction.ExchangeTransaction): Future[RunScriptResult] = handlingErrors {
    blockchainService
      .runAssetScript { RunAssetScriptRequest(assetId = asset.toPB, transaction = Some(input.toPB)) }
      .map(parse)
  }

  override def hasScript(address: Address): Future[Boolean] = handlingErrors {
    blockchainService.hasAddressScript { HasAddressScriptRequest(address = address.toPB) }.map(_.has)
  }

  override def runScript(address: Address, input: Order): Future[RunScriptResult] = handlingErrors {
    blockchainService
      .runAddressScript { RunAddressScriptRequest(address = address.toPB, order = Some(input.toPB)) }
      .map(parse)
  }

  override def wereForged(txIds: Seq[ByteStr]): Future[Map[ByteStr, Boolean]] =
    handlingErrors {
      blockchainService
        .getStatuses { TransactionsByIdRequest(txIds.map(id => ByteString copyFrom id.arr)) }
        .map { _.transactionsStatutes.map(txStatus => txStatus.id.toVanilla -> txStatus.status.isConfirmed).toMap }
    } recover { case _ => txIds.map(_ -> false).toMap }

  override def broadcastTx(tx: transaction.ExchangeTransaction): Future[Boolean] =
    handlingErrors { blockchainService.broadcast { BroadcastRequest(transaction = Some(tx.toPB)) }.map(_.isValid) } recover { case _ => false }

  override def forgedOrder(orderId: ByteStr): Future[Boolean] = handlingErrors {
    blockchainService.forgedOrder { ForgedOrderRequest(orderId.toPB) }.map(_.isForged)
  }

  override def close(): Future[Unit] = {
    shuttingDown.set(true)
    balanceChangesObserver.close()
    channel.shutdown()
    channel.awaitTermination(500, TimeUnit.MILLISECONDS)
    // See NettyChannelBuilder.eventLoopGroup
    eventLoopGroup.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).asScala.map(_ => ())
  }

  override def getNodeAddress: Future[InetAddress] = handlingErrors {
    blockchainService.getNodeAddress { Empty() } map { r =>
      InetAddress.getByName(r.address)
    }
  }

  private final class BalanceChangesObserver extends ClientResponseObserver[Empty, BalanceChangesResponse] with AutoCloseable {
    private val isConnectionEstablished: AtomicBoolean         = new AtomicBoolean(true)
    private var requestStream: ClientCallStreamObserver[Empty] = _

    override def onCompleted(): Unit = log.info("Balance changes stream completed!")

    override def onNext(value: BalanceChangesResponse): Unit = {
      if (isConnectionEstablished.compareAndSet(false, true)) {
        blockchainService.getNodeAddress { Empty() } foreach { response =>
          log.info(s"gRPC connection restored! DEX server now is connected to Node with an address: ${response.address}")
        }
      }

      if (value.batch.nonEmpty) spendableBalanceChangesSubject.onNext(groupByAddress(value))
    }

    override def onError(e: Throwable): Unit = if (!shuttingDown.get()) {
      if (isConnectionEstablished.compareAndSet(true, false)) log.error("Connection with Node lost!", e)
      channel.resetConnectBackoff()
      requestBalanceChanges()
    }

    override def close(): Unit = if (requestStream != null) requestStream.cancel("Shutting down", new StatusRuntimeException(Status.CANCELLED))

    override def beforeStart(requestStream: ClientCallStreamObserver[Empty]): Unit = this.requestStream = requestStream
  }
}
