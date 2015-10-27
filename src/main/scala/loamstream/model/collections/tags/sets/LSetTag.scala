package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.heaps.LHeapTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LSetTag extends LHeapTag {
  override def plusKey[TC: TypeTag]: LSetTag
}
