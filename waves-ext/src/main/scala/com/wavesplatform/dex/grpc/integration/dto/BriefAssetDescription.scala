package com.wavesplatform.dex.grpc.integration.dto

import com.wavesplatform.transaction.assets.exchange.AssetPair

case class BriefAssetDescription(name: String, decimals: Int, hasScript: Boolean, nft: Option[Boolean] = None) {

  def isNft: Boolean = nft.getOrElse(false)

}

object BriefAssetDescription {

  val wavesDescription: BriefAssetDescription =
    BriefAssetDescription(
      name = AssetPair.WavesName,
      decimals = 8,
      hasScript = false,
      nft = Some(false)
    )

  val someWavesDescription: Option[BriefAssetDescription] = Option(wavesDescription)
}
