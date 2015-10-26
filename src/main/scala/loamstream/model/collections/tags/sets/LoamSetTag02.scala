package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.heaps.{LoamHeapTag02, LoamHeapTag01}
import loamstream.model.collections.tags.keys.{LoamKeyTag02, LoamKeyTag01}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamSetTag02 {
  def create[K00: TypeTag, K01:TypeTag] = LoamSetTag02(typeTag[K00], typeTag[K01])
}

case class LoamSetTag02[K00, K01](kTag00: TypeTag[K00], kTag01: TypeTag[K01])
  extends LoamSetTag with LoamHeapTag02[K00, K01] {
  override def key = LoamKeyTag02[K00, K01](kTag00, kTag01)

  override def plusKey[K02: TypeTag] = LoamSetTag03[K00, K01, K02](kTag00, kTag01, typeTag[K02])
}
