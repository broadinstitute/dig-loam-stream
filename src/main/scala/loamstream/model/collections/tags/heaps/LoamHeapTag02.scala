package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.{LoamKeyTag02, LoamKeyTag01}

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LoamHeapTag02[K00, K01] extends LoamHeapTag with LoamKeyTag02.HasKeyTag02[K00, K01] {
  def plusKey[K02: TypeTag]: LoamHeapTag03[K00, K01, K02]
}
