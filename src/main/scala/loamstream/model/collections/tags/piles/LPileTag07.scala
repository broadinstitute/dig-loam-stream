package loamstream.model.collections.tags.piles

import loamstream.model.collections.tags.keys.LKeyTag07

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LPileTag07[K00, K01, K02, K03, K04, K05, K06]
  extends LPileTag with LKeyTag07.HasKeyTag07[K00, K01, K02, K03, K04, K05, K06] {
  //  def plusKey[K07: TypeTag]: LHeapTag08[K00, K01, K02, K03, K04, K05, K06]
    def plusKey[K07: TypeTag]: LPileTag // TODO
}
