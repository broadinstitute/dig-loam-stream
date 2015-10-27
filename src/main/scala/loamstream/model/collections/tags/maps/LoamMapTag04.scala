package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.heaps.LoamHeapTag04
import loamstream.model.collections.tags.keys.LoamKeyTag04
import loamstream.model.collections.tags.sets.LoamSetTag04

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamMapTag04 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag, V: TypeTag] =
    LoamMapTag04(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03], typeTag[V])
}

case class LoamMapTag04[K00, K01, K02, K03, V](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02],
                                               kTag03: TypeTag[K03], vTag: TypeTag[V])
  extends LoamMapTag[V] with LoamHeapTag04[K00, K01, K02, K03] {
  override def plusKey[K04: TypeTag] = LoamMapTag05(kTag00, kTag01, kTag02, kTag03, typeTag[K04], vTag)

  override def key = LoamKeyTag04(kTag00, kTag01, kTag02, kTag03)

  override def toSet = LoamSetTag04(kTag00, kTag01, kTag02, kTag03)
}
