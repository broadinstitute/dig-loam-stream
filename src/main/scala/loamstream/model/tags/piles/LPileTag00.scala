package loamstream.model.tags.piles

import loamstream.model.tags.keys.LKeyTag00
import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag00 extends LPileTag with LKeyTag00.HasKeyTag00 {
  type UpTag[_] <: LPileTag01[_]
}
