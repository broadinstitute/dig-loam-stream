package loamstream.model.streams.pots.methods

import loamstream.model.streams.methods.LMethod1I1O
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
case class LMethodPot1I1O[I0C <: LPileTag, O0C <: LPileTag, KN: TypeTag](id: String, child: LMethod1I1O[I0C, O0C])
  extends LMethodPot[LMethod1I1O[I0C, O0C]] with LMethod1I1O[I0C#UpTag[KN], O0C#UpTag[KN]] {
  def tag = child.tag.plusKey[KN]
}
