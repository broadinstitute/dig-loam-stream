package loamstream.model.streams.pots.methods

import loamstream.model.streams.methods.{LMethod1I1O, LMethod1I0O}
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
case class LMethodPot1I1O[I0C <: LPileTag, O0 <: LPileTag, KN: TypeTag](id: String, child: LMethod1I1O[I0C, O0])
  extends LMethodPot[LMethod1I1O[I0C, O0]] with LMethod1I1O[I0C#UpTag[KN], O0] {
//  def tag = child.tag.plusKeyI0[KN]
  def tag = ???
}
