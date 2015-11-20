package loamstream.model.tags.piles

import loamstream.model.tags.keys.LKeyTag02

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag02[K00, K01] extends LPileTag with LKeyTag02.HasKeyTag02[K00, K01] {
  type UpTag[_] <: LPileTag03[K00, K01, _]
}
