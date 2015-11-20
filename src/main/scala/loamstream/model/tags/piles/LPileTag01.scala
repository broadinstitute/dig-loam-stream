package loamstream.model.tags.piles

import loamstream.model.tags.keys.LKeyTag01
import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag01[K00] extends LPileTag with LKeyTag01.HasKeyTag01[K00] {
  type UpTag[_] <: LPileTag02[K00, _]
}
