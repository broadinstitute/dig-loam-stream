package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.heaps.LoamHeapTag01
import loamstream.model.collections.tags.keys.LoamKeyTag01

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamSetTag01 {
  def create[K00: TypeTag] = LoamSetTag01(typeTag[K00])
}

case class LoamSetTag01[K00](kTag00: TypeTag[K00]) extends LoamSetTag with LoamHeapTag01[K00] {
  override def key = LoamKeyTag01[K00](kTag00)

  override def plusKey[K01: TypeTag] = LoamSetTag02[K00, K01](kTag00, typeTag[K01])
}
