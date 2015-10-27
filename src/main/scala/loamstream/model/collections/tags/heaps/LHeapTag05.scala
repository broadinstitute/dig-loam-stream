package loamstream.model.collections.tags.heaps

import loamstream.model.collections.tags.keys.{LKeyTag05, LKeyTag04}

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LHeapTag05[K00, K01, K02, K03, K04] extends LHeapTag with LKeyTag05.HasKeyTag05[K00, K01, K02, K03, K04] {
  def plusKey[K05: TypeTag]: LHeapTag06[K00, K01, K02, K03, K04, K05]
}
