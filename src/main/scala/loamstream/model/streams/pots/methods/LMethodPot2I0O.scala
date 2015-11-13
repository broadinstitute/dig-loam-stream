package loamstream.model.streams.pots.methods

import loamstream.model.streams.methods.LMethod2I0O
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
case class LMethodPot2I0O[I0C <: LPileTag, I1C <: LPileTag, KN: TypeTag](id: String, child: LMethod2I0O[I0C, I1C])
  extends LMethodPot[LMethod2I0O[I0C, I1C]] with LMethod2I0O[I0C#UpTag[KN], I1C#UpTag[KN]] {
  def tag = child.tag.plusKey[KN]
}
