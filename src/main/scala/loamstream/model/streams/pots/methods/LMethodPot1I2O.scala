package loamstream.model.streams.pots.methods

import loamstream.model.streams.methods.LMethod1I2O
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
case class LMethodPot1I2O[I0C <: LPileTag, O0C <: LPileTag, O1C <: LPileTag,
KN: TypeTag](id: String, child: LMethod1I2O[I0C, O0C, O1C])
  extends LMethodPot[LMethod1I2O[I0C, O0C, O1C]] with LMethod1I2O[I0C#UpTag[KN], O0C#UpTag[KN], O1C#UpTag[KN]] {
  def tag = child.tag.plusKey[KN]
}
