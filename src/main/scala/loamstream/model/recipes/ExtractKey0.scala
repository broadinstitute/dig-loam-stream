package loamstream.model.recipes

import loamstream.model.calls.props.LProps
import loamstream.model.calls.{LPileCall, LSetCall}
import loamstream.model.recipes.ExtractKey0.CanExtractKey0
import loamstream.model.tags.{LKeys, LPileTag, LSetTag}

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 1/8/2016.
  */
object ExtractKey0 {

  trait CanExtractKey0 extends LProps

  implicit class Extraction[K: TypeTag, Tag0 <: LPileTag[K, _], Call0 <: LPileCall[Tag0, _, _]](call0: Call0) {
    type Tag = LSetTag.Set1[K]
    type Call = LSetCall[Tag, LPileCalls.LCalls1[Tag0, Call0], _]
    def key1(id: String): Call = LSetCall(LSetTag.forKeyTup1[K], ExtractKey0(LPileCalls.calls1(call0)))
  }

  def fromPile[K, Tag0 <: LPileTag[K, _], Call0 <: LPileCall[Tag0, _, CanExtractKey0]](pile: Call0) = {
    ExtractKey0[K, Tag0, Call0](LPileCalls.calls1[Tag0, Call0](pile))
  }
}

case class ExtractKey0[K, Tag0 <: LPileTag[K, _],
Call0 <: LPileCall[Tag0, _, CanExtractKey0]](inputs: LPileCalls.LCalls1[Tag0, Call0])
  extends LRecipe[LPileCalls.LCalls1[Tag0, Call0]] {
}
