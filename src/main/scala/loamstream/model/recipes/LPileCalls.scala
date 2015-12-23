package loamstream.model.recipes

import loamstream.model.calls.LPileCall
import loamstream.model.tags.LPileTag

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LPileCalls[PileTag0 <: LPileTag[_, _], PileCall0 <: LPileCall[_], MorePileCalls <: LPileCalls[_, _, _]] {
  def ::[PileTagNew <: LPileTag[_, _], PileCallNew[_] <: LPileCall[_]](pileCallNew: PileCallNew[PileTagNew]) =
    LPileCallsNode[PileTagNew, PileCallNew, LPileCalls[PileTag0, PileCall0, MorePileCalls]](pileCallNew, this)
}

object LPileCallsNil extends LPileCalls[Nothing, Nothing, Nothing]

case class LPileCallsNode[PileTag0 <: LPileTag[_, _], PileCall0[_] <: LPileCall[_],
MorePileCalls <: LPileCalls[_, _, _]](pileCall0: PileCall0[PileTag0], tail: MorePileCalls)