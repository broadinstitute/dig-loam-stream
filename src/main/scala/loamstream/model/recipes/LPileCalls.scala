package loamstream.model.recipes

import loamstream.model.calls.LPileCall
import loamstream.model.tags.LPileTag

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LPileCalls {
  type LCalls0 = LPileCallsNil.type
  type LCalls1[Tag0 <: LPileTag[_, _], Call0 <: LPileCall[Tag0, _, _]] = LPileCallsNode[Tag0, Call0, LCalls0]
  type LCalls2[Tag0 <: LPileTag[_, _], Call0[Tag] <: LPileCall[Tag, _, _],
  Tag1 <: LPileTag[_, _], Call1 <: LPileCall[Tag1, _, _]] =
  LPileCallsNode[Tag0, Call0, LCalls1[Tag1, Call1]]
  type LCalls3[Tag0 <: LPileTag[_, _], Call0[Tag] <: LPileCall[Tag, _, _],
  Tag1 <: LPileTag[_, _], Call1[Tag] <: LPileCall[Tag, _, _],
  Tag2 <: LPileTag[_, _], Call2[Tag] <: LPileCall[Tag, _, _]] =
  LPileCallsNode[Tag0, Call0, LCalls2[Tag1, Call1, Tag2, Call2]]

  def calls0(): LCalls0 = LPileCallsNil

  def calls1[Tag0 <: LPileTag[_, _], Call0 <: LPileCall[Tag0, _, _]](call0: Call0): LCalls1[Tag0, Call0] =
    LPileCallsNode[Tag0, Call0, LCalls0](call0, calls0())

  def calls2[Tag0 <: LPileTag[_, _], Call0[Tag] <: LPileCall[Tag, _, _],
  Tag1 <: LPileTag[_, _], Call1[Tag] <: LPileCall[Tag, _, _]](call0: Call0[Tag0], call1: Call1[Tag1]):
  LCalls2[Tag0, Call0, Tag1, Call1] =
    LPileCallsNode[Tag0, Call0, LCalls1[Tag1, Call1]](call0, calls1(call1))

  def calls3[Tag0 <: LPileTag[_, _], Call0[Tag] <: LPileCall[Tag, _, _],
  Tag1 <: LPileTag[_, _], Call1[Tag] <: LPileCall[Tag, _, _],
  Tag2 <: LPileTag[_, _], Call2[Tag] <: LPileCall[Tag, _, _]](call0: Call0[Tag0], call1: Call1[Tag1], call2: Call2[Tag2]):
  LCalls3[Tag0, Call0, Tag1, Call1, Tag2, Call2] =
    LPileCallsNode[Tag0, Call0, LCalls2[Tag1, Call1, Tag2, Call2]](call0, calls2(call1, call2))
}

trait LPileCalls[Call0 <: LPileCall[_, _, _], MoreCalls <: LPileCalls[_, _]] {
  def ::[TagNew <: LPileTag[_, _], CallNew[Tag] <: LPileCall[Tag, _, _]](callNew: CallNew[TagNew]) =
    LPileCallsNode[TagNew, CallNew, LPileCalls[Call0, MoreCalls]](callNew, this)
}

object LPileCallsNil extends LPileCalls[Nothing, Nothing]

case class LPileCallsNode[Tag0 <: LPileTag[_, _], Call0 <: LPileCall[Tag0, _, _],
MoreCalls <: LPileCalls[_, _]](call0: Call0[Tag0], tail: MoreCalls) extends LPileCalls[Call0, MoreCalls]