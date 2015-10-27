package loamstream.model.collections.tags.piles

import loamstream.model.collections.tags.keys.LKeyTag02

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LPileTag02[K00, K01] extends LPileTag with LKeyTag02.HasKeyTag02[K00, K01] {
  def plusKey[K02: TypeTag]: LPileTag03[K00, K01, K02]
}
