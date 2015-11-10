package loamstream.model.tags.piles

import loamstream.model.tags.keys.LKeyTag07

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag07[K00, K01, K02, K03, K04, K05, K06]
  extends LPileTag with LKeyTag07.HasKeyTag07[K00, K01, K02, K03, K04, K05, K06] {
  //  def plusKey[K07: TypeTag]: LHeapTag08[K00, K01, K02, K03, K04, K05, K06]
  type UpTag[_] <: LPileTag // TODO
}
