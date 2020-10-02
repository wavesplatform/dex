package com.wavesplatform.dex.settings

import cats.syntax.apply._
import com.wavesplatform.dex.settings.utils.ConfigSettingsValidator
import com.wavesplatform.dex.settings.utils.ConfigSettingsValidator._
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ValueReader

/** Represents market order restrictions. Field values are in percents */
case class DeviationsSettings(enable: Boolean, profit: Double, loss: Double, fee: Double)
// TODO: maxPriceProfit, maxPriceLoss, maxFeeDeviation


object DeviationsSettings {

  implicit val deviationsSettingsReader: ValueReader[DeviationsSettings] = { (cfg, path) =>
    val cfgValidator = ConfigSettingsValidator(cfg)

    def validateDeviationPercent(settingName: String): ErrorsListOr[Double] = {
      cfgValidator.validateByPredicate[Double](settingName)(_ > 0, "required 0 < percent")
    }

    (
      cfgValidator.validate[Boolean](s"$path.enable"),
      validateDeviationPercent(s"$path.profit"),
      validateDeviationPercent(s"$path.loss"),
      validateDeviationPercent(s"$path.fee")
    ) mapN DeviationsSettings.apply getValueOrThrowErrors
  }
}
