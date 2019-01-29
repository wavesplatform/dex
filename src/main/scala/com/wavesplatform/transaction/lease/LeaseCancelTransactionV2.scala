package com.wavesplatform.transaction.lease

import com.google.common.primitives.Bytes
import com.wavesplatform.account.{AddressScheme, PrivateKeyAccount, PublicKeyAccount}
import com.wavesplatform.common.state.ByteStr
import com.wavesplatform.common.utils.EitherExt2
import com.wavesplatform.crypto
import com.wavesplatform.transaction.ValidationError.{GenericError, UnsupportedVersion}
import com.wavesplatform.transaction._
import com.wavesplatform.transaction.description._
import monix.eval.Coeval

import scala.util.{Failure, Success, Try}

case class LeaseCancelTransactionV2 private (version: Byte,
                                             chainId: Byte,
                                             sender: PublicKeyAccount,
                                             leaseId: ByteStr,
                                             fee: Long,
                                             timestamp: Long,
                                             proofs: Proofs)
    extends LeaseCancelTransaction
    with FastHashId {

  override def chainByte: Option[Byte] = Some(chainId)

  override val builder: TransactionParser = LeaseCancelTransactionV2

  val bodyBytes: Coeval[Array[Byte]] =
    Coeval.evalOnce(Bytes.concat(Array(builder.typeId, version, chainId), bytesBase()))

  override val bytes = Coeval.evalOnce(Bytes.concat(Array(0: Byte), bodyBytes(), proofs.bytes()))

}

object LeaseCancelTransactionV2 extends TransactionParserFor[LeaseCancelTransactionV2] with TransactionParser.MultipleVersions {

  override val typeId: Byte = LeaseCancelTransaction.typeId

  override def supportedVersions: Set[Byte] = Set(2)
  private def currentChainId                = AddressScheme.current.chainId

  override protected def parseTail(version: Byte, bytes: Array[Byte]): Try[TransactionT] =
    Try {
      val chainId                                = bytes(0)
      val (sender, fee, timestamp, leaseId, end) = LeaseCancelTransaction.parseBase(bytes, 1)
      (for {
        proofs <- Proofs.fromBytes(bytes.drop(end))
        tx     <- LeaseCancelTransactionV2.create(version, chainId, sender, leaseId, fee, timestamp, proofs)
      } yield tx).fold(left => Failure(new Exception(left.toString)), right => Success(right))
    }.flatten

  def create(version: Byte,
             chainId: Byte,
             sender: PublicKeyAccount,
             leaseId: ByteStr,
             fee: Long,
             timestamp: Long,
             proofs: Proofs): Either[ValidationError, TransactionT] =
    for {
      _ <- Either.cond(supportedVersions.contains(version), (), UnsupportedVersion(version))
      _ <- Either.cond(chainId == currentChainId, (), GenericError(s"Wrong chainId actual: ${chainId.toInt}, expected: $currentChainId"))
      _ <- LeaseCancelTransaction.validateLeaseCancelParams(leaseId, fee)
    } yield LeaseCancelTransactionV2(version, chainId, sender, leaseId, fee, timestamp, proofs)

  def signed(version: Byte,
             chainId: Byte,
             sender: PublicKeyAccount,
             leaseId: ByteStr,
             fee: Long,
             timestamp: Long,
             signer: PrivateKeyAccount): Either[ValidationError, TransactionT] = {
    create(version, chainId, sender, leaseId, fee, timestamp, Proofs.empty).right.map { unsigned =>
      unsigned.copy(proofs = Proofs.create(Seq(ByteStr(crypto.sign(signer, unsigned.bodyBytes())))).explicitGet())
    }
  }

  def selfSigned(version: Byte,
                 chainId: Byte,
                 sender: PrivateKeyAccount,
                 leaseId: ByteStr,
                 fee: Long,
                 timestamp: Long): Either[ValidationError, TransactionT] = {
    signed(version, chainId, sender, leaseId, fee, timestamp, sender)
  }

  val byteDescription: ByteEntity[LeaseCancelTransactionV2] = {
    (
      ConstantByte(1, value = 0, name = "Transaction multiple version mark") ~
        ConstantByte(2, value = typeId, name = "Transaction type") ~
        ConstantByte(3, value = 2, name = "Version") ~
        OneByte(4, "Chain ID") ~
        PublicKeyAccountBytes(5, "Sender's public key") ~
        LongBytes(6, "Fee") ~
        LongBytes(7, "Timestamp") ~
        ByteStrDefinedLength(8, "Lease ID", crypto.DigestSize) ~
        ProofsBytes(9)
    ).map {
      case ((((((((_, _), version), chainId), senderPublicKey), fee), timestamp), leaseId), proofs) =>
        LeaseCancelTransactionV2(
          version = version,
          chainId = chainId,
          sender = senderPublicKey,
          leaseId = leaseId,
          fee = fee,
          timestamp = timestamp,
          proofs = proofs
        )
    }
  }
}
