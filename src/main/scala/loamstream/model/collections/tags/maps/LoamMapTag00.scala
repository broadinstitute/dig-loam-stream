package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.heaps.LoamHeapTag00
import loamstream.model.collections.tags.keys.LoamKeyTag00
import loamstream.model.collections.tags.sets.LoamSetTag00

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamMapTag00 {
  def create[V: TypeTag] = LoamMapTag00(typeTag[V])
}

case class LoamMapTag00[V](vTag: TypeTag[V]) extends LoamMapTag[V] with LoamHeapTag00 {
  override def plusKey[TC: TypeTag]: LoamMapTag01 = ???

  override def key = LoamKeyTag00

  override def toSet = LoamSetTag00
}
