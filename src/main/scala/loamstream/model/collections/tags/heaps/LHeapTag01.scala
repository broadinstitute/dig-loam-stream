package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LKeyTag01

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LHeapTag01[K00] extends LHeapTag with LKeyTag01.HasKeyTag01[K00] {
  def plusKey[K01: TypeTag]: LHeapTag02[K00, K01]
}
