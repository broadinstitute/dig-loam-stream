package loamstream.model.recipes

import loamstream.model.calls.LPileCall
import loamstream.model.calls.props.LProps
import loamstream.model.recipes.ExtractKey0.CanExtractKey0
import loamstream.model.tags.LPileTag

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 1/8/2016.
  */
object ExtractKey0 {

  trait CanExtractKey0 extends LProps

  def fromPile[K,Inputs0 <: LPileCalls[_, _, _], Tag0 <: LPileTag[K, _], Call0[Tag <: LPileTag[_, _]] <: LPileCall[Tag, Inputs0, CanExtractKey0]
  ](pile: Call0[Tag0]) = {
    ExtractKey0[K, Inputs0, Tag0, Call0](LPileCalls.calls1[Tag0, Call0](pile))
  }
}

case class ExtractKey0[K, Inputs0 <: LPileCalls[_, _, _], Tag0 <: LPileTag[K, _],
Call0[Tag <: LPileTag[_, _]] <: LPileCall[Tag, Inputs0, CanExtractKey0]](inputs: LPileCalls.LCalls1[Tag0, Call0])
  extends LRecipe[LPileCalls.LCalls1[Tag0, Call0]] {
}
