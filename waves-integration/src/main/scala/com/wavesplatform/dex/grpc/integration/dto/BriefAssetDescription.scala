package com.wavesplatform.dex.grpc.integration.dto

import com.wavesplatform.dex.domain.asset.Asset

case class BriefAssetDescription(name: String, decimals: Int, hasScript: Boolean)

object BriefAssetDescription {

  val wavesDescription: BriefAssetDescription =
    BriefAssetDescription(
      name = Asset.WavesName,
      decimals = 8,
      hasScript = false
    )

  val someWavesDescription: Option[BriefAssetDescription] = Option(wavesDescription)
}
