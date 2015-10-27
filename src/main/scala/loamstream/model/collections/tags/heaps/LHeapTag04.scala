package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LKeyTag04

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LHeapTag04[K00, K01, K02, K03] extends LHeapTag with LKeyTag04.HasKeyTag04[K00, K01, K02, K03] {
  def plusKey[K04: TypeTag]: LHeapTag05[K00, K01, K02, K03, K04]
}
