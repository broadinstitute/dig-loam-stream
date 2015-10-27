package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LoamKeyTag04

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LoamHeapTag04[K00, K01, K02, K03] extends LoamHeapTag with LoamKeyTag04.HasKeyTag04[K00, K01, K02, K03] {
  def plusKey[K04: TypeTag]: LoamHeapTag05[K00, K01, K02, K03, K04]
}
