package loamstream.model.tags.piles

import loamstream.model.tags.keys.LKeyTag06

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LPileTag06[K00, K01, K02, K03, K04, K05]
  extends LPileTag with LKeyTag06.HasKeyTag06[K00, K01, K02, K03, K04, K05] {
  def plusKey[K06: TypeTag]: LPileTag07[K00, K01, K02, K03, K04, K05, K06]
}
