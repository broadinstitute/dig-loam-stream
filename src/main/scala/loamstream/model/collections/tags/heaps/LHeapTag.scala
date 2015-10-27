package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LKeyTag.HasKeyTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LHeapTag extends HasKeyTag {
  def plusKey[TC: TypeTag]: LHeapTag
}
