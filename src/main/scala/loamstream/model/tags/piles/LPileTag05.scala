package loamstream.model.tags.piles

import loamstream.model.tags.keys.{LKeyTag05, LKeyTag04}

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LPileTag05[K00, K01, K02, K03, K04] extends LPileTag with LKeyTag05.HasKeyTag05[K00, K01, K02, K03, K04] {
  def plusKey[K05: TypeTag]: LPileTag06[K00, K01, K02, K03, K04, K05]
}
