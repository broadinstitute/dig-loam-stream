package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LoamKeyTag01

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LoamHeapTag01[K00] extends LoamHeapTag with LoamKeyTag01.HasKeyTag01[K00] {
  def plusKey[K01: TypeTag]: LoamHeapTag02[K00, K01]
}
