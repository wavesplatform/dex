package com.wavesplatform.dex.grpc.integration.services

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap

import cats.syntax.either._
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
import com.wavesplatform.transaction.Asset.IssuedAsset
import com.wavesplatform.transaction.TxValidationError.GenericError
import com.wavesplatform.transaction.smart.script.ScriptRunner
import com.wavesplatform.utils.ScorexLogging
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import monix.eval.{Coeval, Task}
import monix.execution.{CancelableFuture, Scheduler}
import shapeless.Coproduct

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class WavesBlockchainApiGrpcService(context: ExtensionContext, balanceChangesBatchLingerMs: FiniteDuration)(implicit sc: Scheduler)
    extends WavesBlockchainApiGrpc.WavesBlockchainApi
    with AutoCloseable
    with ScorexLogging {

  // A clean logic requires more actions, see DEX-606
  private val balanceChangesSubscribers = ConcurrentHashMap.newKeySet[StreamObserver[BalanceChangesResponse]](2)
  private val balanceChanges: Coeval[CancelableFuture[Unit]] = Coeval.evalOnce {
    context.spendableBalanceChanged
      .bufferTimed(balanceChangesBatchLingerMs)
      .map { changesBuffer =>
        val vanillaBatch = changesBuffer.distinct.map { case (address, asset) => (address, asset, context.utx.spendableBalance(address, asset)) }
        vanillaBatch.map { case (address, asset, balance) => BalanceChangesResponse.Record(address.toPB, asset.toPB, balance) }
      }
      .filter(_.nonEmpty)
      .map(BalanceChangesResponse.apply)
      .doOnSubscriptionCancel(Task {
        // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
        val shutdownError = new StatusRuntimeException(Status.UNAVAILABLE) // Because it should try to connect to other DEX Extension
        balanceChangesSubscribers.forEach(_.onError(shutdownError))
        balanceChangesSubscribers.clear()
      })
      .doOnComplete(Task {
        // For consistency
        balanceChangesSubscribers.forEach(_.onCompleted())
        balanceChangesSubscribers.clear()
      })
      .foreach { x =>
        balanceChangesSubscribers.forEach(_.onNext(x))
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
        else context.broadcastTransaction(tx).resultE.map(BroadcastResponse.apply).leftFlatMap(_ => BroadcastResponse().asRight)
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

  override def spendableAssetBalance(request: SpendableAssetBalanceRequest): Future[SpendableAssetBalanceResponse] = Future {
    val addr    = Address.fromBytes(request.address.toVanilla).explicitGetErr()
    val assetId = request.assetId.toVanillaAsset
    SpendableAssetBalanceResponse(context.utx.spendableBalance(addr, assetId))
  }

  override def forgedOrder(request: ForgedOrderRequest): Future[ForgedOrderResponse] = Future {
    val seen = context.blockchain.filledVolumeAndFee(request.orderId.toVanilla).volume > 0
    ForgedOrderResponse(isForged = seen)
  }

  override def getBalanceChanges(request: Empty, responseObserver: StreamObserver[BalanceChangesResponse]): Unit =
    if (!balanceChanges().isCompleted) balanceChangesSubscribers.add(responseObserver)

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

  override def getNodeAddress(request: Empty): Future[NodeAddressResponse] = Future {
    NodeAddressResponse(InetAddress.getLocalHost.getHostAddress)
  }

  override def close(): Unit = balanceChanges.foreachL(_.cancel())
}
