package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.heaps.LoamHeapTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LoamSetTag extends LoamHeapTag {
  override def plusKey[TC: TypeTag]: LoamSetTag
}
