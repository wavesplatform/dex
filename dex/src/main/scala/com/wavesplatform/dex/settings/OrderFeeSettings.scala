package com.wavesplatform.dex.settings

import cats.data.Validated.Valid
import cats.instances.string._
import cats.syntax.apply._
import cats.syntax.foldable._
import com.wavesplatform.dex.domain.asset.Asset
import com.wavesplatform.dex.domain.asset.Asset.{Waves, _}
import com.wavesplatform.dex.settings.AssetType
import com.wavesplatform.dex.settings.FeeMode
import com.wavesplatform.dex.settings.utils.ConfigCursorsOps.ConfigCursorOps
import com.wavesplatform.dex.settings.utils.{ConfigCursorsOps, ConfigSettingsValidator}
import com.wavesplatform.dex.settings.utils.ConfigSettingsValidator.ErrorsListOr
import com.wavesplatform.dex.settings.utils.{ConfigCursorsOps, ConfigSettingsValidator, WrappedDescendantHint}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.EnumerationReader._
import net.ceedubs.ficus.readers.ValueReader
import play.api.libs.json.{Reads, Writes}
import pureconfig.ConfigReader
import pureconfig.generic.auto._
import cats.syntax.either._
import enumeratum._

sealed trait OrderFeeSettings extends Product with Serializable

object OrderFeeSettings extends ConfigCursorsOps {

  final case class DynamicSettings(baseMakerFee: Long, baseTakerFee: Long) extends OrderFeeSettings {
    val maxBaseFee: Long   = math.max(baseMakerFee, baseTakerFee)
    val makerRatio: Double = (BigDecimal(baseMakerFee) / maxBaseFee).toDouble
    val takerRatio: Double = (BigDecimal(baseTakerFee) / maxBaseFee).toDouble
  }

  object DynamicSettings {
    def symmetric(baseFee: Long): DynamicSettings = DynamicSettings(baseFee, baseFee)
  }

  final case class FixedSettings(defaultAsset: Asset, minFee: Long)      extends OrderFeeSettings
  final case class PercentSettings(assetType: AssetType, minFee: Double) extends OrderFeeSettings

  implicit val orderFeeSettingsHint = new WrappedDescendantHint[OrderFeeSettings]("mode") {
    override protected def fieldValue(name: String): String = name.dropRight("Settings".length).toLowerCase
  }

  implicit val orderFeeSettingsReader: ValueReader[OrderFeeSettings] = { (cfg, path) =>
    val cfgValidator = ConfigSettingsValidator(cfg)

    def getPrefixByMode(mode: FeeMode): String = s"$path.$mode"

    def validateDynamicSettings: ErrorsListOr[DynamicSettings] = {
      val prefix = getPrefixByMode(FeeMode.Dynamic)
      (
        cfgValidator.validateByPredicate[Long](s"$prefix.base-maker-fee")(predicate = fee => 0 < fee, errorMsg = s"required 0 < base maker fee"),
        cfgValidator.validateByPredicate[Long](s"$prefix.base-taker-fee")(predicate = fee => 0 < fee, errorMsg = s"required 0 < base taker fee"),
      ) mapN DynamicSettings.apply
    }

    def validateFixedSettings: ErrorsListOr[FixedSettings] = {

      val prefix         = getPrefixByMode(FeeMode.Fixed)
      val feeValidator   = cfgValidator.validateByPredicate[Long](s"$prefix.min-fee") _
      val assetValidated = cfgValidator.validate[Asset](s"$prefix.asset")

      val feeValidated = assetValidated match {
        case Valid(Waves) => feeValidator(fee => 0 < fee, s"required 0 < fee")
        case _            => feeValidator(_ > 0, "required 0 < fee")
      }

      (assetValidated, feeValidated) mapN FixedSettings
    }

    def validatePercentSettings: ErrorsListOr[PercentSettings] = {
      val prefix = getPrefixByMode(FeeMode.Percent)
      (
        cfgValidator.validate[AssetType](s"$prefix.asset-type"),
        cfgValidator.validatePercent(s"$prefix.min-fee")
      ) mapN PercentSettings
    }

    def getSettingsByMode(mode: FeeMode): ErrorsListOr[OrderFeeSettings] = mode match {
      case FeeMode.Dynamic => validateDynamicSettings
      case FeeMode.Fixed   => validateFixedSettings
      case FeeMode.Percent => validatePercentSettings
    }

    cfgValidator.validate[FeeMode](s"$path.mode").toEither flatMap (mode => getSettingsByMode(mode).toEither) match {
      case Left(errorsAcc)         => throw new Exception(errorsAcc.mkString_(", "))
      case Right(orderFeeSettings) => orderFeeSettings
    }
  }

//  import pureconfig.generic.FieldCoproductHint
//  implicit val feeSettingsHint: FieldCoproductHint[OrderFeeSettings] = new FieldCoproductHint[OrderFeeSettings]("mode")

//  implicit val orderFeeSettingsReader1: ConfigReader[OrderFeeSettings] = { cur =>
//    def getPrefixByMode(mode: FeeMode): String = s"$mode"
//
//    def validateDynamicSettings: Result[DynamicSettings] = {
//      val prefix: String = FeeMode.DYNAMIC.toString
//      for {
//        baseMakerFee <- cur.as[Long](s"$prefix.base-maker-fee").ensure(cur.failed(s"required 0 < base maker fee"))(_ > 0)
//        baseTakerFee <- cur.as[Long](s"$prefix.base-taker-fee").ensure(cur.failed(s"required 0 < base taker fee"))(_ > 0)
//      } yield DynamicSettings(baseMakerFee, baseTakerFee)
//    }
//
//    def validateFixedSettings: ErrorsListOr[FixedSettings] = {
//
//      val prefix         = getPrefixByMode(FeeMode.FIXED)
//      val feeValidator   = cfgValidator.validateByPredicate[Long](s"$prefix.min-fee") _
//      val assetValidated = cfgValidator.validate[Asset](s"$prefix.asset")
//
//      val feeValidated = assetValidated match {
//        case Valid(Waves) => feeValidator(fee => 0 < fee, s"required 0 < fee")
//        case _            => feeValidator(_ > 0, "required 0 < fee")
//      }
//
//      (assetValidated, feeValidated) mapN FixedSettings
//    }
//
//    def validatePercentSettings: ErrorsListOr[PercentSettings] = {
//      val prefix = getPrefixByMode(FeeMode.PERCENT)
//      (
//        cfgValidator.validate[AssetType](s"$prefix.asset-type"),
//        cfgValidator.validatePercent(s"$prefix.min-fee")
//      ) mapN PercentSettings
//    }
//
//    def getSettingsByMode(mode: FeeMode): Result[OrderFeeSettings] = mode match {
//      case FeeMode.DYNAMIC => validateDynamicSettings
//      case FeeMode.FIXED   => validateFixedSettings
//      case FeeMode.PERCENT => validatePercentSettings
//    }
//
//    for {
//      mode     <- cur.as[FeeMode]("mode")
//      settings <- getSettingsByMode(mode)
//    } yield settings
//
////    cfgValidator.validate[FeeMode](s"$path.mode").toEither flatMap (mode => getSettingsByMode(mode).toEither) match {
////      case Left(errorsAcc)         => throw new Exception(errorsAcc.mkString_(", "))
////      case Right(orderFeeSettings) => orderFeeSettings
////    }
//  }

}

sealed trait AssetType extends EnumEntry
object AssetType extends Enum[AssetType] with PlayLowercaseJsonEnum[AssetType] {
  val values = findValues

  case object Amount    extends AssetType
  case object Price     extends AssetType
  case object Spending  extends AssetType
  case object Receiving extends AssetType

  implicit val valueReader: ValueReader[AssetType] = ??? // TODO REMOVE
}

sealed trait FeeMode extends EnumEntry
object FeeMode extends Enum[FeeMode] with PlayLowercaseJsonEnum[FeeMode] {
  val values = findValues

  case object Dynamic extends FeeMode
  case object Fixed   extends FeeMode
  case object Percent extends FeeMode

  implicit val valueReader: ValueReader[FeeMode] = ??? // TODO REMOVE
}
