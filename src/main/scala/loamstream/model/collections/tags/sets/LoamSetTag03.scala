package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.heaps.LoamHeapTag03
import loamstream.model.collections.tags.keys.LoamKeyTag03

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamSetTag03 {
  def create[K00: TypeTag, K01:TypeTag, K02:TypeTag] = LoamSetTag03(typeTag[K00], typeTag[K01], typeTag[K02])
}

case class LoamSetTag03[K00, K01, K02](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02])
  extends LoamSetTag with LoamHeapTag03[K00, K01, K02] {
  override def key = LoamKeyTag03[K00, K01, K02](kTag00, kTag01, kTag02)

  override def plusKey[K03: TypeTag] = LoamSetTag04[K00, K01, K02, K03](kTag00, kTag01,  kTag02, typeTag[K03])
}
