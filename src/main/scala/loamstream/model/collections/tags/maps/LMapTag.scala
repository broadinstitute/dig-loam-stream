package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.heaps.LHeapTag
import loamstream.model.collections.tags.sets.LSetTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LMapTag[V] extends LHeapTag {

  def vTag: TypeTag[V]

  def toSet: LSetTag

  override def plusKey[KN: TypeTag]: LMapTag[V]
}
