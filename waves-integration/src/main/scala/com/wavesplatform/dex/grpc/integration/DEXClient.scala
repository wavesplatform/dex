package com.wavesplatform.dex.grpc.integration

import java.util.concurrent.TimeUnit

import com.wavesplatform.dex.grpc.integration.clients.async.WavesBalancesGrpcAsyncClient
import com.wavesplatform.dex.grpc.integration.clients.sync.WavesBlockchainGrpcSyncClient
import io.grpc._
import io.grpc.internal.DnsNameResolverProvider
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global

class DEXClient(target: String, val scheduler: Scheduler = global) {

  private val channel =
    ManagedChannelBuilder
      .forTarget(target)
      .maxHedgedAttempts(10)
      .maxRetryAttempts(20)
      .keepAliveWithoutCalls(true)
      .keepAliveTime(2, TimeUnit.SECONDS)
      .keepAliveTimeout(1, TimeUnit.SECONDS)
      .nameResolverFactory(new DnsNameResolverProvider)
      .defaultLoadBalancingPolicy("pick_first")
      .usePlaintext()
      .build

  lazy val wavesBlockchainSyncClient = new WavesBlockchainGrpcSyncClient(channel)
  lazy val wavesBalancesAsyncClient  = new WavesBalancesGrpcAsyncClient(channel)(scheduler)
}
