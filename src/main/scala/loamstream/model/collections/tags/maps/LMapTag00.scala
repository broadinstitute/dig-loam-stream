package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.heaps.LHeapTag00
import loamstream.model.collections.tags.keys.LKeyTag00
import loamstream.model.collections.tags.sets.LSetTag00

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LMapTag00 {
  def create[V: TypeTag] = LMapTag00(typeTag[V])
}

case class LMapTag00[V](vTag: TypeTag[V]) extends LMapTag[V] with LHeapTag00 {
  override def plusKey[K00: TypeTag] = LMapTag01[K00, V](typeTag[K00], vTag)

  override def key = LKeyTag00

  override def toSet = LSetTag00
}
