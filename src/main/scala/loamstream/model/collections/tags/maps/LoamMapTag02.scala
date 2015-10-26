package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.heaps.{LoamHeapTag02, LoamHeapTag01}
import loamstream.model.collections.tags.keys.{LoamKeyTag02, LoamKeyTag01}
import loamstream.model.collections.tags.sets.{LoamSetTag02, LoamSetTag01}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamMapTag02 {
  def create[K00: TypeTag, K01: TypeTag, V: TypeTag] = LoamMapTag02(typeTag[K00], typeTag[K01], typeTag[V])
}

case class LoamMapTag02[K00, K01, V](kTag00: TypeTag[K00], kTag01: TypeTag[K01], vTag: TypeTag[V])
  extends LoamMapTag[V] with LoamHeapTag02[K00, K01] {
  override def plusKey[K02: TypeTag] = LoamMapTag03(kTag00, kTag01, typeTag[K02], vTag)

  override def key = LoamKeyTag02(kTag00, kTag01)

  override def toSet = LoamSetTag02(kTag00, kTag01)
}
