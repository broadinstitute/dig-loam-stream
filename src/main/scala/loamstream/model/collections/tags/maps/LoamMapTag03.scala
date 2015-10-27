package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.heaps.LoamHeapTag03
import loamstream.model.collections.tags.keys.LoamKeyTag03
import loamstream.model.collections.tags.sets.LoamSetTag03

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamMapTag03 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, V: TypeTag] =
    LoamMapTag03(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[V])
}

case class LoamMapTag03[K00, K01, K02, V](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02],
                                          vTag: TypeTag[V])
  extends LoamMapTag[V] with LoamHeapTag03[K00, K01, K02] {
  override def plusKey[K03: TypeTag] = LoamMapTag04(kTag00, kTag01, kTag02, typeTag[K03], vTag)

  override def key = LoamKeyTag03(kTag00, kTag01, kTag02)

  override def toSet = LoamSetTag03(kTag00, kTag01, kTag02)
}
