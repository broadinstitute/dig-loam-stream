package loamstream.model.recipes

import loamstream.model.calls.LPileCall
import loamstream.model.tags.LTags

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LPileCalls {
  type LCalls0 = LPileCallsNil.type
  type LCalls1[Call0 <: LPileCall[_, _, _]] = LPileCallsNode[Call0, LCalls0]
  type LCalls2[Call0 <: LPileCall[_, _, _], Call1 <: LPileCall[_, _, _]] = LPileCallsNode[Call0, LCalls1[Call1]]
  type LCalls3[Call0 <: LPileCall[_, _, _], Call1 <: LPileCall[_, _, _], Call2 <: LPileCall[_, _, _]] =
  LPileCallsNode[Call0, LCalls2[Call1, Call2]]

  def calls0(): LCalls0 = LPileCallsNil

  def calls1[Call0 <: LPileCall[_, _, _]](call0: Call0): LCalls1[Call0] =
    LPileCallsNode[Call0, LCalls0](call0, calls0())

  def calls2[Call0 <: LPileCall[_, _, _], Call1 <: LPileCall[_, _, _]](call0: Call0, call1: Call1):
  LCalls2[Call0, Call1] =
    LPileCallsNode[Call0, LCalls1[Call1]](call0, calls1(call1))

  def calls3[Call0 <: LPileCall[_, _, _], Call1 <: LPileCall[_, _, _],
  Call2 <: LPileCall[_, _, _]](call0: Call0, call1: Call1, call2: Call2):
  LCalls3[Call0, Call1, Call2] =
    LPileCallsNode[Call0, LCalls2[Call1, Call2]](call0, calls2(call1, call2))
}

trait LPileCalls[Call0 <: LPileCall[_, _, _], MoreCalls <: LPileCalls[_, _]] {
  def ::[TagNew <: LTags, CallNew <: LPileCall[TagNew, _, _]](callNew: CallNew) =
    LPileCallsNode[CallNew, LPileCalls[Call0, MoreCalls]](callNew, this)
}

object LPileCallsNil extends LPileCalls[Nothing, Nothing]

case class LPileCallsNode[Call0 <: LPileCall[_, _, _],
MoreCalls <: LPileCalls[_, _]](call0: Call0, tail: MoreCalls) extends LPileCalls[Call0, MoreCalls]