package com.wavesplatform.dex.tool.connectors

import java.io.File

import cats.instances.either._
import cats.syntax.either._
import cats.syntax.option._
import com.typesafe.config.ConfigFactory._
import com.wavesplatform.dex.cli.{ErrorOr, _}
import com.wavesplatform.dex.db.AccountStorage
import com.wavesplatform.dex.domain.account.{AddressScheme, KeyPair}
import com.wavesplatform.dex.error.Implicits.ThrowableOps
import com.wavesplatform.dex.settings.MatcherSettings.valueReader
import com.wavesplatform.dex.settings.{MatcherSettings, loadConfig}
import com.wavesplatform.dex.tool.connectors.SuperConnector.Env
import net.ceedubs.ficus.Ficus._

import scala.util.Try

case class SuperConnector private (env: Env,
                                   dexRest: DexRestConnector,
                                   nodeRest: NodeRestConnector,
                                   dexExtensionGrpc: DexExtensionGrpcConnector,
                                   dexWs: DexWsConnector,
                                   authServiceRest: Option[AuthServiceRestConnector])
    extends AutoCloseable {

  def close(): Unit = {
    Seq(dexRest, nodeRest, dexExtensionGrpc, dexWs).foreach { _.close() }
    authServiceRest.foreach { _.close() }
  }
}

// noinspection ScalaStyle
object SuperConnector {

  private[tool] final case class Env(chainId: Byte, matcherSettings: MatcherSettings, matcherKeyPair: KeyPair)

  private val processLeftIndent = 110

  def create(dexConfigPath: String, dexRestApi: String, nodeRestApi: String, authServiceRestApi: Option[String]): ErrorOr[SuperConnector] = {

    def prependScheme(uri: String): String = {
      val uriWithoutSlash = if (uri.last == '/') uri.init else uri
      val (plain, secure) = "http://" -> "https://"
      if (uri.startsWith(secure) || uri.startsWith(plain)) uriWithoutSlash else plain + uriWithoutSlash
    }

    def logProcessing[A](processing: String)(f: => ErrorOr[A]): ErrorOr[A] = wrapByLogs(s"  $processing... ", "Done\n", processLeftIndent.some)(f)

    def loadMatcherSettings(confPath: String): ErrorOr[MatcherSettings] =
      Try {
        val matcherSettings: MatcherSettings = loadConfig { parseFile(new File(dexConfigPath)) }.as[MatcherSettings]("waves.dex")
        AddressScheme.current = new AddressScheme { override val chainId: Byte = matcherSettings.addressSchemeCharacter.toByte }
        matcherSettings
      }.toEither.leftMap(ex => s"Cannot load matcher settings by path $confPath: ${ex.getWithStackTrace}")

    def loadMatcherKeyPair(accountStorage: AccountStorage.Settings): ErrorOr[KeyPair] =
      AccountStorage.load(accountStorage).bimap(ex => s"Cannot load Matcher account! $ex", _.keyPair)

    for {
      _               <- log("\nSetting up the Super Connector:\n")
      matcherSettings <- logProcessing("Loading Matcher settings") { loadMatcherSettings(dexConfigPath) }
      matcherKeyPair  <- logProcessing("Loading Matcher Key Pair") { loadMatcherKeyPair(matcherSettings.accountStorage) }

      dexRestIpAndPort = s"${matcherSettings.restApi.address}:${matcherSettings.restApi.port}"
      dexRestApiUri    = prependScheme(if (dexRestApi.isEmpty) dexRestIpAndPort else dexRestApi)
      dexRestConnector = DexRestConnector(dexRestApiUri)
      _ <- logProcessing(s"Setting up connection with DEX REST API (${dexRestConnector.repeatRequestOptions})")(success)

      chainId = AddressScheme.current.chainId
      nodeRestApiUri = prependScheme(
        if (nodeRestApi.isEmpty) matcherSettings.wavesBlockchainClient.grpc.target.split(":").head + ":6869" else nodeRestApi
      )
      nodeRestConnector = NodeRestConnector(nodeRestApiUri, chainId)
      _ <- logProcessing(s"Setting up connection with Node REST API (${nodeRestConnector.repeatRequestOptions})")(success)

      extensionGrpcApiUri = matcherSettings.wavesBlockchainClient.grpc.target

      dexExtensionGrpcConnector <- logProcessing("Setting up connection with DEX Extension gRPC API") {
        DexExtensionGrpcConnector.create(extensionGrpcApiUri)
      }

      dexWsApiUri = s"${if (dexRestApiUri startsWith "http://") "ws://" else "wss://"}$dexRestIpAndPort/ws/v0"

      (dexWsConnector, wsInitial) <- logProcessing("Setting up connection with DEX WS API")(
        for {
          wsConnector <- DexWsConnector.create(dexWsApiUri)
          wsInitial   <- wsConnector.receiveInitialMessage
        } yield wsConnector -> wsInitial
      )

      mayBeAuthServiceRestApiUri = authServiceRestApi map prependScheme
      mayBeAuthServiceConnector  = mayBeAuthServiceRestApiUri map { AuthServiceRestConnector(_, chainId) }
      _ <- logProcessing("Setting up connection with Auth Service REST API")(success)

      env = Env(chainId, matcherSettings, matcherKeyPair)
      superConnector = SuperConnector(
        env,
        dexRestConnector,
        nodeRestConnector,
        dexExtensionGrpcConnector,
        dexWsConnector,
        mayBeAuthServiceConnector
      )

      _ <- log(
        s"""
           |Super Connector created!
           |
           |DEX configurations:
           |  Chain ID               : $chainId (${chainId.toChar})
           |  Matcher public key     : ${matcherKeyPair.publicKey.toString}
           |  Matcher address        : ${matcherKeyPair.publicKey.toAddress}
           |  DEX REST API           : $dexRestApiUri
           |  Node REST API          : $nodeRestApiUri
           |  DEX extension gRPC API : $extensionGrpcApiUri
           |  DEX WS API             : $dexWsApiUri, connection ID = ${wsInitial.connectionId}
           |  Auth Service REST API  : ${mayBeAuthServiceRestApiUri.getOrElse("Target wasn't provided, account updates check will not be performed!")}
       """.stripMargin
      )
    } yield superConnector
  }
}
