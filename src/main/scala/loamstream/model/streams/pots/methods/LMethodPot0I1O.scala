package loamstream.model.streams.pots.methods

import loamstream.model.streams.methods.{LMethod0I1O, LMethod1I0O}
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
case class LMethodPot0I1O[O0C <: LPileTag, KN: TypeTag](id: String, child: LMethod0I1O[O0C])
  extends LMethodPot[LMethod0I1O[O0C]] with LMethod0I1O[O0C#UpTag[KN]] {
//  def tag = child.tag.plusKeyO0[KN]
  def tag = ???
}
