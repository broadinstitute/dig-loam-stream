package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LKeyTag02

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LHeapTag02[K00, K01] extends LHeapTag with LKeyTag02.HasKeyTag02[K00, K01] {
  def plusKey[K02: TypeTag]: LHeapTag03[K00, K01, K02]
}
