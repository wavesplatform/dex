package com.wavesplatform.dex.fp

import cats.Group
import cats.instances.map.catsKernelStdMonoidForMap
import cats.kernel.Semigroup

object MapImplicits {
  implicit def group[K, V](implicit vGroup: Group[V]): Group[Map[K, V]] = new Group[Map[K, V]] {
    override def inverse(a: Map[K, V]): Map[K, V]               = a.map { case (k, v) => k -> vGroup.inverse(v) }
    override def empty: Map[K, V]                               = Map.empty
    override def combine(x: Map[K, V], y: Map[K, V]): Map[K, V] = catsKernelStdMonoidForMap[K, V].combine(x, y)
  }

  /**
    * @return ∀ (k, v) ∈ A |+| B, v != 0
    */
  implicit def cleaningGroup[K, V](implicit vGroup: Group[V]): Group[Map[K, V]] = new Group[Map[K, V]] {
    override def inverse(a: Map[K, V]): Map[K, V] = a.map { case (k, v) => k -> vGroup.inverse(v) }
    override def empty: Map[K, V]                 = Map.empty
    override def combine(xs: Map[K, V], ys: Map[K, V]): Map[K, V] = {
      val (lessXs, biggerXs) = if (xs.size <= ys.size) (xs, ys) else (ys, xs)
      lessXs.foldLeft(biggerXs) {
        case (r, (k, v)) =>
          val updatedV = Semigroup.maybeCombine(v, r.get(k))
          if (updatedV == vGroup.empty) r - k
          else r.updated(k, updatedV)
      }
    }
  }
}
