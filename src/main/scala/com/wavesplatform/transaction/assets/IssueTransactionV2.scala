package com.wavesplatform.transaction.assets

import com.google.common.primitives.Bytes
import com.wavesplatform.account.{AddressScheme, PrivateKeyAccount, PublicKeyAccount}
import com.wavesplatform.common.state.ByteStr
import com.wavesplatform.crypto
import com.wavesplatform.serialization.Deser
import com.wavesplatform.transaction.ValidationError.{GenericError, UnsupportedVersion}
import com.wavesplatform.transaction._
import com.wavesplatform.transaction.description._
import com.wavesplatform.transaction.smart.script.{Script, ScriptReader}
import monix.eval.Coeval
import play.api.libs.json.{JsObject, Json}

import scala.util.Try

case class IssueTransactionV2 private (version: Byte,
                                       chainId: Byte,
                                       sender: PublicKeyAccount,
                                       name: Array[Byte],
                                       description: Array[Byte],
                                       quantity: Long,
                                       decimals: Byte,
                                       reissuable: Boolean,
                                       script: Option[Script],
                                       fee: Long,
                                       timestamp: Long,
                                       proofs: Proofs)
    extends IssueTransaction
    with FastHashId
    with ChainSpecific {
  override val builder: TransactionParser = IssueTransactionV2
  override val bodyBytes: Coeval[Array[Byte]] = Coeval.evalOnce(
    Bytes.concat(
      Array(builder.typeId, version, chainId),
      bytesBase(),
      Deser.serializeOption(script)(s => s.bytes().arr)
    ))
  override val bytes: Coeval[Array[Byte]] = Coeval.evalOnce(Bytes.concat(Array(0: Byte), bodyBytes(), proofs.bytes()))

  override val json: Coeval[JsObject] = Coeval.evalOnce(issueJson() ++ Json.obj("chainId" -> chainId, "script" -> script.map(_.bytes().base64)))
}

object IssueTransactionV2 extends TransactionParserFor[IssueTransactionV2] with TransactionParser.MultipleVersions {

  override val typeId: Byte                 = IssueTransaction.typeId
  override val supportedVersions: Set[Byte] = Set(2)

  private def currentChainId = AddressScheme.current.chainId

  override protected def parseTail(version: Byte, bytes: Array[Byte]): Try[TransactionT] =
    Try {
      val chainId                                                                                       = bytes(0)
      val (sender, assetName, description, quantity, decimals, reissuable, fee, timestamp, scriptStart) = IssueTransaction.parseBase(bytes, 1)
      val (scriptOptEi: Option[Either[ValidationError.ScriptParseError, Script]], scriptEnd) =
        Deser.parseOption(bytes, scriptStart)(ScriptReader.fromBytes)
      val scriptEiOpt: Either[ValidationError.ScriptParseError, Option[Script]] = scriptOptEi match {
        case None            => Right(None)
        case Some(Right(sc)) => Right(Some(sc))
        case Some(Left(err)) => Left(err)
      }

      (for {
        proofs <- Proofs.fromBytes(bytes.drop(scriptEnd))
        script <- scriptEiOpt
        tx <- IssueTransactionV2
          .create(version, chainId, sender, assetName, description, quantity, decimals, reissuable, script, fee, timestamp, proofs)
      } yield tx).left.map(e => new Throwable(e.toString)).toTry

    }.flatten

  def create(version: Byte,
             chainId: Byte,
             sender: PublicKeyAccount,
             name: Array[Byte],
             description: Array[Byte],
             quantity: Long,
             decimals: Byte,
             reissuable: Boolean,
             script: Option[Script],
             fee: Long,
             timestamp: Long,
             proofs: Proofs): Either[ValidationError, TransactionT] =
    for {
      _ <- Either.cond(supportedVersions.contains(version), (), UnsupportedVersion(version))
      _ <- Either.cond(chainId == currentChainId, (), GenericError(s"Wrong chainId actual: ${chainId.toInt}, expected: $currentChainId"))
      _ <- IssueTransaction.validateIssueParams(name, description, quantity, decimals, reissuable, fee)
    } yield IssueTransactionV2(version, chainId, sender, name, description, quantity, decimals, reissuable, script, fee, timestamp, proofs)

  def signed(version: Byte,
             chainId: Byte,
             sender: PublicKeyAccount,
             name: Array[Byte],
             description: Array[Byte],
             quantity: Long,
             decimals: Byte,
             reissuable: Boolean,
             script: Option[Script],
             fee: Long,
             timestamp: Long,
             signer: PrivateKeyAccount): Either[ValidationError, TransactionT] =
    for {
      unverified <- create(version, chainId, sender, name, description, quantity, decimals, reissuable, script, fee, timestamp, Proofs.empty)
      proofs     <- Proofs.create(Seq(ByteStr(crypto.sign(signer, unverified.bodyBytes()))))
    } yield unverified.copy(proofs = proofs)

  def selfSigned(version: Byte,
                 chainId: Byte,
                 sender: PrivateKeyAccount,
                 name: Array[Byte],
                 description: Array[Byte],
                 quantity: Long,
                 decimals: Byte,
                 reissuable: Boolean,
                 script: Option[Script],
                 fee: Long,
                 timestamp: Long): Either[ValidationError, TransactionT] =
    signed(version, chainId, sender, name, description, quantity, decimals, reissuable, script, fee, timestamp, sender)

  val byteDescription: ByteEntity[IssueTransactionV2] = {
    (
      ConstantByte(1, value = 0, name = "Transaction multiple version mark") ~
        ConstantByte(2, value = typeId, name = "Transaction type") ~
        ConstantByte(3, value = 2, name = "Version") ~
        OneByte(4, "Chain ID") ~
        PublicKeyAccountBytes(5, "Sender's public key") ~
        BytesArrayUndefinedLength(6, "Name") ~
        BytesArrayUndefinedLength(7, "Description") ~
        LongBytes(8, "Quantity") ~
        OneByte(9, "Decimals") ~
        BooleanByte(10, "Reissuable") ~
        LongBytes(11, "Fee") ~
        LongBytes(12, "Timestamp") ~
        OptionScriptBytes(13, "Script") ~
        ProofsBytes(14)
    ).map {
      case (((((((((((((_, _), version), chainId), senderPublicKey), name), desc), quantity), decimals), reissuable), fee), timestamp), script),
            proofs) =>
        IssueTransactionV2(
          version = version,
          chainId = chainId,
          sender = senderPublicKey,
          name = name,
          description = desc,
          quantity = quantity,
          decimals = decimals,
          reissuable = reissuable,
          script = script,
          fee = fee,
          timestamp = timestamp,
          proofs = proofs
        )
    }
  }
}
