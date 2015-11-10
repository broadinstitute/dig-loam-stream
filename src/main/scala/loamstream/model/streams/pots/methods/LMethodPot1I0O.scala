package loamstream.model.streams.pots.methods

import loamstream.model.streams.methods.LMethod1I0O
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
case class LMethodPot1I0O[I0 <: LPileTag, +C <: LMethod1I0O[_]](id: String, child: C)
  extends LMethodPot[C] with LMethod1I0O[I0] {
  def input0 = ??? // TODO
  def tag = ??? // TODO
}
