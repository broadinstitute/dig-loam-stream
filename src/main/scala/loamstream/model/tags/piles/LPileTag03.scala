package loamstream.model.tags.piles

import loamstream.model.tags.keys.LKeyTag03

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag03[K00, K01, K02] extends LPileTag with LKeyTag03.HasKeyTag03[K00, K01, K02] {
  type UpTag[_] <: LPileTag04[K00, K01, K02, _]
}
