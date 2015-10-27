package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.LKeyTag03

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LHeapTag03[K00, K01, K02] extends LHeapTag with LKeyTag03.HasKeyTag03[K00, K01, K02] {
  def plusKey[K03: TypeTag]: LHeapTag04[K00, K01, K02, K03]
}
