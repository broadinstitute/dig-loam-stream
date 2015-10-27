package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.heaps.LHeapTag03
import loamstream.model.collections.tags.keys.LKeyTag03
import loamstream.model.collections.tags.sets.LSetTag03

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LMapTag03 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, V: TypeTag] =
    LMapTag03(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[V])
}

case class LMapTag03[K00, K01, K02, V](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02],
                                          vTag: TypeTag[V])
  extends LMapTag[V] with LHeapTag03[K00, K01, K02] {
  override def plusKey[K03: TypeTag] = LMapTag04(kTag00, kTag01, kTag02, typeTag[K03], vTag)

  override def key = LKeyTag03(kTag00, kTag01, kTag02)

  override def toSet = LSetTag03(kTag00, kTag01, kTag02)
}
