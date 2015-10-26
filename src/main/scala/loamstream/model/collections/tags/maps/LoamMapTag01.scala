package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.heaps.LoamHeapTag01
import loamstream.model.collections.tags.keys.LoamKeyTag01
import loamstream.model.collections.tags.sets.LoamSetTag01

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamMapTag01 {
  def create[K00: TypeTag, V: TypeTag] = LoamMapTag01(typeTag[K00], typeTag[V])
}

case class LoamMapTag01[K00, V](kTag00: TypeTag[K00], vTag: TypeTag[V]) extends LoamMapTag[V] with LoamHeapTag01[K00] {
  override def plusKey[K01: TypeTag] = LoamMapTag02(kTag00, typeTag[K01], vTag)

  override def key = LoamKeyTag01(kTag00)

  override def toSet = LoamSetTag01(kTag00)
}
