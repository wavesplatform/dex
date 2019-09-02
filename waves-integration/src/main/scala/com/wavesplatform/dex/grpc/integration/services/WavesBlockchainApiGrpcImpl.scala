package com.wavesplatform.dex.grpc.integration.services

import cats.syntax.either._
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import com.wavesplatform.account.Address
import com.wavesplatform.dex.grpc.integration.error
import com.wavesplatform.dex.grpc.integration.protobuf.ToVanillaConversions._
import com.wavesplatform.dex.grpc.integration.protobuf.{EitherVEExt, StreamObserverMonixOps}
import com.wavesplatform.dex.grpc.integration.smart.MatcherScriptRunner
import com.wavesplatform.lang.ValidationError
import com.wavesplatform.lang.v1.compiler.Terms
import com.wavesplatform.lang.v1.compiler.Terms.{FALSE, TRUE}
import com.wavesplatform.protobuf.transaction.VanillaTransaction
import com.wavesplatform.state.Blockchain
import com.wavesplatform.transaction.Asset.IssuedAsset
import com.wavesplatform.transaction.TxValidationError.GenericError
import com.wavesplatform.transaction.smart.script.ScriptRunner
import com.wavesplatform.utils.ScorexLogging
import com.wavesplatform.utx.UtxPool
import io.grpc.stub.StreamObserver
import monix.execution.Scheduler
import monix.reactive.Observable
import shapeless.Coproduct

import scala.concurrent.Future
import scala.util.control.NonFatal

class WavesBlockchainApiGrpcImpl(blockchain: Blockchain, utx: UtxPool, broadcastTransaction: VanillaTransaction => Unit)(implicit sc: Scheduler)
    extends WavesBlockchainApiGrpc.WavesBlockchainApi
    with ScorexLogging {

  override def getStatuses(request: TransactionsByIdRequest, responseObserver: StreamObserver[TransactionStatus]): Unit = {
    val result = Observable(request.transactionIds: _*).map { txId =>
      blockchain.transactionHeight(txId.toVanilla) match {
        case Some(height) => TransactionStatus(txId, TransactionStatus.Status.CONFIRMED, height) // TODO
        case None =>
          utx.transactionById(txId.toVanilla) match {
            case Some(_) => TransactionStatus(txId, TransactionStatus.Status.UNCONFIRMED)
            case None    => TransactionStatus(txId, TransactionStatus.Status.NOT_EXISTS)
          }
      }
    }
    responseObserver.completeWith(result)
  }

  override def broadcast(request: BroadcastRequest): Future[BroadcastResponse] = Future {
    request.transaction
      .fold[Either[ValidationError, SignedExchangeTransaction]](GenericError("The signed transaction must be specified").asLeft)(_.asRight)
      .flatMap(_.toVanilla)
      .flatMap { tx =>
        val r = utx
          .putIfNew(tx)
          .map { _ =>
            broadcastTransaction(tx)
            BroadcastResponse(isValid = true)
          }
          .resultE
          .leftFlatMap(_ => BroadcastResponse().asRight)
        r
      }
      .explicitGetErr()
  }

  override def isFeatureActivated(request: IsFeatureActivatedRequest): Future[IsFeatureActivatedResponse] = Future {
    IsFeatureActivatedResponse(isActivated = blockchain.activatedFeatures.contains(request.featureId.toShort))
  }

  override def assetDescription(request: AssetIdRequest): Future[AssetDescriptionResponse] = Future {
    import AssetDescriptionResponse._

    val desc = blockchain.assetDescription(IssuedAsset(request.assetId.toVanilla))
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
    HasScriptResponse(has = blockchain.hasAssetScript(IssuedAsset(request.assetId.toVanilla)))
  }

  override def runAssetScript(request: RunAssetScriptRequest): Future[RunScriptResponse] = Future {
    import RunScriptResponse._

    val asset = IssuedAsset(request.assetId.toVanilla)

    val r = blockchain.assetScript(asset) match {
      case None => Result.Empty
      case Some(script) =>
        val tx = request.transaction
          .getOrElse(throw new IllegalArgumentException("Expected a transaction"))
          .toVanilla
          .getOrElse(throw new IllegalArgumentException("Can't parse the transaction"))
        parseScriptResult(ScriptRunner(Coproduct(tx), blockchain, script, isAssetScript = true, asset.id)._2)
    }
    RunScriptResponse(r)
  }

  override def hasAddressScript(request: HasAddressScriptRequest): Future[HasScriptResponse] = Future {
    Address
      .fromBytes(request.address.toVanilla)
      .map { addr =>
        HasScriptResponse(has = blockchain.hasScript(addr))
      }
      .explicitGetErr()
  }

  override def runAddressScript(request: RunAddressScriptRequest): Future[RunScriptResponse] = Future {
    import RunScriptResponse._

    val address = Address.fromBytes(request.address.toVanilla).explicitGetErr()
    val r = blockchain.accountScript(address) match {
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
    SpendableAssetBalanceResponse(blockchain.balance(addr, assetId))
  }

  override def forgedOrder(request: ForgedOrderRequest): Future[ForgedOrderResponse] = Future {
    val seen = blockchain.filledVolumeAndFee(request.orderId.toVanilla).volume > 0
    ForgedOrderResponse(isForged = seen)
  }

  private def parseScriptResult(raw: Either[String, Terms.EVALUATED]): RunScriptResponse.Result = {
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
}
