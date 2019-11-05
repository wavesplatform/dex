package com.wavesplatform.dex.grpc.integration.caches

import java.time.Duration

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

abstract class BlockchainCache[K <: AnyRef, V <: AnyRef](loader: K => V, expiration: Option[Duration]) {

  lazy private val cache: LoadingCache[K, V] = {
    val builder = CacheBuilder.newBuilder
    expiration
      .fold(builder)(builder.expireAfterWrite)
      .build {
        new CacheLoader[K, V] {
          override def load(key: K): V = loader(key)
        }
      }
  }

  def get(key: K): V = cache.get(key)

  def put(key: K, value: V): Unit = cache.put(key, value)

  def size: Long = cache.size()
}