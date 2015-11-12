package loamstream.model.streams.pots.methods

import loamstream.model.streams.methods.LMethod1I0O
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
case class LMethodPot1I0O[I0C <: LPileTag, +C[_] <: LMethod1I0O[_], KN](id: String, child: C[I0C])
  extends LMethodPot[C[I0C]] with LMethod1I0O[I0C#UpTag[KN]] {

  def tag = ??? // TODO
}
