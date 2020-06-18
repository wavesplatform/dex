package com.wavesplatform.dex.grpc.integration.services

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap

import cats.syntax.either._
import cats.syntax.monoid._
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import com.wavesplatform.account.Address
import com.wavesplatform.dex.grpc.integration._
import com.wavesplatform.dex.grpc.integration.protobuf.EitherVEExt
import com.wavesplatform.dex.grpc.integration.protobuf.PbToWavesConversions._
import com.wavesplatform.dex.grpc.integration.protobuf.WavesToPbConversions._
import com.wavesplatform.dex.grpc.integration.smart.MatcherScriptRunner
import com.wavesplatform.extensions.{Context => ExtensionContext}
import com.wavesplatform.features.BlockchainFeatureStatus
import com.wavesplatform.features.FeatureProvider._
import com.wavesplatform.lang.ValidationError
import com.wavesplatform.lang.v1.compiler.Terms
import com.wavesplatform.lang.v1.compiler.Terms.{FALSE, TRUE}
import com.wavesplatform.transaction.Asset
import com.wavesplatform.transaction.Asset.{IssuedAsset, Waves}
import com.wavesplatform.transaction.TxValidationError.GenericError
import com.wavesplatform.transaction.smart.script.ScriptRunner
import com.wavesplatform.utils.ScorexLogging
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{Status, StatusRuntimeException}
import monix.eval.{Coeval, Task}
import monix.execution.{CancelableFuture, Scheduler}
import shapeless.Coproduct

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

// TODO remove balanceChangesBatchLingerMs parameter after release 2.1.2
class WavesBlockchainApiGrpcService(context: ExtensionContext, balanceChangesBatchLingerMs: FiniteDuration)(implicit sc: Scheduler)
    extends WavesBlockchainApiGrpc.WavesBlockchainApi
    with ScorexLogging {

  private val allSpendableBalances = new ConcurrentHashMap[Address, Map[Asset, Long]]()

  // A clean logic requires more actions, see DEX-606
  // TODO remove after release 2.1.2
  private val balanceChangesSubscribers = ConcurrentHashMap.newKeySet[StreamObserver[BalanceChangesResponse]](2)
  // TODO rename to balanceChangesSubscribers after release 2.1.2
  private val realTimeBalanceChangesSubscribers = ConcurrentHashMap.newKeySet[StreamObserver[BalanceChangesFlattenResponse]](2)

  private val cleanupTask: Task[Unit] = Task {
    log.info("Closing balance changes stream...")
    // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
    val shutdownError = new StatusRuntimeException(Status.UNAVAILABLE) // Because it should try to connect to other DEX Extension

    // TODO remove after release 2.1.2
    balanceChangesSubscribers.forEach { _.onError(shutdownError) }
    balanceChangesSubscribers.clear()

    realTimeBalanceChangesSubscribers.forEach { _.onError(shutdownError) }
    realTimeBalanceChangesSubscribers.clear()
  }

  private val realTimeBalanceChanges: Coeval[CancelableFuture[Unit]] = Coeval.evalOnce {
    context.spendableBalanceChanged
      .map {
        case (address, asset) =>
          val newAssetBalance = context.utx.spendableBalance(address, asset)
          val addressBalance  = allSpendableBalances.getOrDefault(address, Map.empty)
          val needUpdate      = !addressBalance.get(asset).contains(newAssetBalance)

          if (needUpdate) {
            allSpendableBalances.put(address, addressBalance + (asset -> newAssetBalance))
            Some(BalanceChangesFlattenResponse(address.toPB, asset.toPB, newAssetBalance))
          } else Option.empty[BalanceChangesFlattenResponse]
      }
      .collect { case Some(response) => response }
      .doOnSubscriptionCancel(cleanupTask)
      .doOnComplete(cleanupTask)
      .foreach { x =>
        realTimeBalanceChangesSubscribers.forEach { subscriber =>
          try subscriber.onNext(x)
          catch { case e: Throwable => log.warn(s"Can't send balance changes to $subscriber", e) }
        }
      }
  }

  // TODO remove after release 2.1.2
  private val balanceChanges: Coeval[CancelableFuture[Unit]] = Coeval.evalOnce {
    context.spendableBalanceChanged
      .bufferTimed(balanceChangesBatchLingerMs)
      .map { changesBuffer =>
        val vanillaBatch = changesBuffer.distinct.map { case (address, asset) => (address, asset, context.utx.spendableBalance(address, asset)) }
        vanillaBatch.map { case (address, asset, balance) => BalanceChangesResponse.Record(address.toPB, asset.toPB, balance) }
      }
      .filter(_.nonEmpty)
      .map(xs => BalanceChangesResponse(xs))
      .doOnSubscriptionCancel(cleanupTask)
      .doOnComplete(cleanupTask)
      .foreach { x =>
        balanceChangesSubscribers.forEach { subscriber =>
          try subscriber.onNext(x)
          catch {
            case e: Throwable => log.warn(s"Can't send balance changes to $subscriber", e)
          }
        }
      }
  }

  override def getStatuses(request: TransactionsByIdRequest): Future[TransactionsStatusesResponse] = Future {
    val statuses = request.transactionIds.map { txId =>
      context.blockchain.transactionHeight(txId.toVanilla) match {
        case Some(height) => TransactionStatus(txId, TransactionStatus.Status.CONFIRMED, height) // TODO
        case None =>
          context.utx.transactionById(txId.toVanilla) match {
            case Some(_) => TransactionStatus(txId, TransactionStatus.Status.UNCONFIRMED)
            case None    => TransactionStatus(txId, TransactionStatus.Status.NOT_EXISTS)
          }
      }
    }
    TransactionsStatusesResponse(statuses)
  }

  override def broadcast(request: BroadcastRequest): Future[BroadcastResponse] = Future {
    request.transaction
      .fold[Either[ValidationError, SignedExchangeTransaction]](GenericError("The signed transaction must be specified").asLeft)(_.asRight)
      .flatMap { _.toVanilla }
      .flatMap { tx =>
        if (context.blockchain.containsTransaction(tx)) Right(BroadcastResponse(isValid = true))
        else context.broadcastTransaction(tx).resultE.map(xs => BroadcastResponse(xs)).leftFlatMap(_ => BroadcastResponse().asRight)
      }
      .explicitGetErr()
  }

  override def isFeatureActivated(request: IsFeatureActivatedRequest): Future[IsFeatureActivatedResponse] = Future {
    IsFeatureActivatedResponse(
      context.blockchain.featureStatus(request.featureId.toShort, context.blockchain.height) == BlockchainFeatureStatus.Activated
    )
  }

  override def assetDescription(request: AssetIdRequest): Future[AssetDescriptionResponse] = Future {
    import AssetDescriptionResponse._

    val desc = context.blockchain.assetDescription(IssuedAsset(request.assetId.toVanilla))
    val gRpcDesc = desc.fold[MaybeDescription](MaybeDescription.Empty) { desc =>
      MaybeDescription.Description(
        AssetDescription(
          name = ByteString.copyFrom(desc.name),
          decimals = desc.decimals,
          hasScript = desc.script.nonEmpty
        )
      )
    }

    AssetDescriptionResponse(gRpcDesc)
  }

  override def hasAssetScript(request: AssetIdRequest): Future[HasScriptResponse] = Future {
    HasScriptResponse(has = context.blockchain.hasAssetScript(IssuedAsset(request.assetId.toVanilla)))
  }

  override def runAssetScript(request: RunAssetScriptRequest): Future[RunScriptResponse] = Future {
    import RunScriptResponse._

    val asset = IssuedAsset(request.assetId.toVanilla)
    val r = context.blockchain.assetScript(asset) match {
      case None => Result.Empty
      case Some(script) =>
        val tx = request.transaction
          .getOrElse(throw new IllegalArgumentException("Expected a transaction"))
          .toVanilla
          .getOrElse(throw new IllegalArgumentException("Can't parse the transaction"))
        parseScriptResult(ScriptRunner(context.blockchain.height, Coproduct(tx), context.blockchain, script, isAssetScript = true, asset.id)._2)
    }
    RunScriptResponse(r)
  }

  override def hasAddressScript(request: HasAddressScriptRequest): Future[HasScriptResponse] = Future {
    Address
      .fromBytes(request.address.toVanilla)
      .map { addr =>
        HasScriptResponse(has = context.blockchain.hasScript(addr))
      }
      .explicitGetErr()
  }

  override def runAddressScript(request: RunAddressScriptRequest): Future[RunScriptResponse] = Future {
    import RunScriptResponse._

    val address = Address.fromBytes(request.address.toVanilla).explicitGetErr()
    val r = context.blockchain.accountScript(address) match {
      case None => Result.Empty
      case Some(script) =>
        val order = request.order.map(_.toVanilla).getOrElse(throw new IllegalArgumentException("Expected an order"))
        parseScriptResult(MatcherScriptRunner(script, order)._2)
    }

    RunScriptResponse(r)
  }

  // TODO remove after release 2.1.3
  override def spendableAssetBalance(request: SpendableAssetBalanceRequest): Future[SpendableAssetBalanceResponse] = Future {
    val addr    = Address.fromBytes(request.address.toVanilla).explicitGetErr()
    val assetId = request.assetId.toVanillaAsset
    SpendableAssetBalanceResponse(context.utx.spendableBalance(addr, assetId))
  }

  override def spendableAssetsBalances(request: SpendableAssetsBalancesRequest): Future[SpendableAssetsBalancesResponse] = Future {

    val address = Address.fromBytes(request.address.toVanilla).explicitGetErr()

    val assetsBalances =
      request.assetIds.map { requestedAssetRecord =>
        SpendableAssetsBalancesResponse.Record(
          requestedAssetRecord.assetId,
          context.utx.spendableBalance(address, requestedAssetRecord.assetId.toVanillaAsset)
        )
      }

    SpendableAssetsBalancesResponse(assetsBalances)
  }

  override def forgedOrder(request: ForgedOrderRequest): Future[ForgedOrderResponse] = Future {
    val seen = context.blockchain.filledVolumeAndFee(request.orderId.toVanilla).volume > 0
    ForgedOrderResponse(isForged = seen)
  }

  // TODO remove after release 2.1.2
  override def getBalanceChanges(request: Empty, responseObserver: StreamObserver[BalanceChangesResponse]): Unit =
    if (!balanceChanges().isCompleted) {
      responseObserver match {
        case x: ServerCallStreamObserver[_] => x.setOnCancelHandler(() => balanceChangesSubscribers remove x)
        case x                              => log.warn(s"Can't register cancel handler for $x")
      }

      balanceChangesSubscribers.add(responseObserver)
    }

  // TODO rename to getBalanceChanges after release 2.1.2
  override def getRealTimeBalanceChanges(request: Empty, responseObserver: StreamObserver[BalanceChangesFlattenResponse]): Unit =
    if (!realTimeBalanceChanges().isCompleted) {
      responseObserver match {
        case x: ServerCallStreamObserver[_] => x.setOnCancelHandler(() => realTimeBalanceChangesSubscribers remove x)
        case x                              => log.warn(s"Can't register cancel handler for $x")
      }
      realTimeBalanceChangesSubscribers.add(responseObserver)
    }

  private def parseScriptResult(raw: => Either[String, Terms.EVALUATED]): RunScriptResponse.Result = {
    import RunScriptResponse.Result
    try raw match {
      case Left(execError) => Result.ScriptError(execError)
      case Right(FALSE)    => Result.Denied(Empty())
      case Right(TRUE)     => Result.Empty
      case Right(x)        => Result.UnexpectedResult(x.toString)
    } catch {
      case NonFatal(e) =>
        log.trace(error.formatStackTrace(e))
        Result.Exception(Exception(e.getClass.getCanonicalName, Option(e.getMessage).getOrElse("No message")))
    }
  }

  override def allAssetsSpendableBalance(request: AddressRequest): Future[AllAssetsSpendableBalanceResponse] = {
    import com.wavesplatform.state.Portfolio.monoid
    Future {

      val address              = request.address.toVanillaAddress
      val pessimisticPortfolio = context.blockchain.portfolio(address) |+| context.utx.pessimisticPortfolio(address)

      val pessimisticPortfolioNonZeroBalances = {
        if (pessimisticPortfolio.balance == 0) pessimisticPortfolio.assets
        else pessimisticPortfolio.assets ++ Map(Waves -> pessimisticPortfolio.balance)
      }

      AllAssetsSpendableBalanceResponse(
        pessimisticPortfolioNonZeroBalances.map {
          case (a, b) => AllAssetsSpendableBalanceResponse.Record(a.toPB, b)
        }.toSeq
      )
    }
  }

  override def getNodeAddress(request: Empty): Future[NodeAddressResponse] = Future {
    NodeAddressResponse(InetAddress.getLocalHost.getHostAddress)
  }
}
