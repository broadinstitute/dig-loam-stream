package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LKeyTag00

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LHeapTag00 extends LHeapTag with LKeyTag00.HasKeyTag00 {
  def plusKey[K00: TypeTag]: LHeapTag01[K00]
}
