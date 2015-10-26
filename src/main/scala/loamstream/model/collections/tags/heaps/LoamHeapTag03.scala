package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.{LoamKeyTag03, LoamKeyTag02, LoamKeyTag01}

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LoamHeapTag03[K00, K01, K02] extends LoamHeapTag with LoamKeyTag03.HasKeyTag03[K00, K01, K02] {
  def plusKey[K03: TypeTag]: LoamHeapTag04[K00, K01, K02, K03]
}
