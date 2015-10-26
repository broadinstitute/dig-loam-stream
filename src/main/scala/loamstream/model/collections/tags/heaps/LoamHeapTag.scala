package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LoamKeyTag.HasKeyTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LoamHeapTag extends HasKeyTag {
  def plusKey[TC: TypeTag]: LoamHeapTag
}
