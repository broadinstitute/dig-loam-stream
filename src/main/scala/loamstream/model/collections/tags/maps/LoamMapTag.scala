package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.heaps.LoamHeapTag
import loamstream.model.collections.tags.sets.LoamSetTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LoamMapTag[V] extends LoamHeapTag {

  def vTag: TypeTag[V]

  def toSet: LoamSetTag

  override def plusKey[KN: TypeTag]: LoamMapTag[V]
}
