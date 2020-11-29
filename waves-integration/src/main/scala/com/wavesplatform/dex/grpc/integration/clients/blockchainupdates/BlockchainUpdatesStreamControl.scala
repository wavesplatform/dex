package com.wavesplatform.dex.grpc.integration.clients.blockchainupdates

trait BlockchainUpdatesStreamControl {
  def restartFrom(height: Int): Unit
  def checkpoint(height: Int): Unit
  // def askNext(): Unit // TODO
  def stop(): Unit
}
