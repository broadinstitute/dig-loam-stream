package loamstream.model.tags.piles

import loamstream.model.tags.keys.LKeyTag04

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag04[K00, K01, K02, K03] extends LPileTag with LKeyTag04.HasKeyTag04[K00, K01, K02, K03] {
  type UpTag[_] <: LPileTag05[K00, K01, K02, K03, _]
}
