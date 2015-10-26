package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LoamKeyTag00

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LoamHeapTag00 extends LoamHeapTag with LoamKeyTag00.HasKeyTag00 {
  def plusKey[K00: TypeTag]: LoamHeapTag01[K00]
}
