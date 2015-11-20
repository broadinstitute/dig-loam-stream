package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1I, Has2O, Namer}
import loamstream.model.streams.methods.LMethod1I2O.{LSocketI0, LSocketO0, LSocketO1}
import loamstream.model.streams.pots.methods.LMethodPot1I2O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I2O
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
object LMethod1I2O {

  case class LSocketI0[I0 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag](method: LMethod1I2O[I0, O0, O1])
    extends LSocket[I0, LMethod1I2O[I0, O0, O1]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketO0[I0 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag](method: LMethod1I2O[I0, O0, O1])
    extends LSocket[O0, LMethod1I2O[I0, O0, O1]] {
    override def pileTag = method.tag.output0
  }

  case class LSocketO1[I0 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag](method: LMethod1I2O[I0, O0, O1])
    extends LSocket[O1, LMethod1I2O[I0, O0, O1]] {
    override def pileTag = method.tag.output1
  }

}

trait LMethod1I2O[I0 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag]
  extends Has1I[I0, LMethod1I2O[I0, O0, O1]] with Has2O[O0, O1, LMethod1I2O[I0, O0, O1]] {
  type MTag = LMethodTag1I2O[I0, O0, O1]
  type Parent[KN] = LMethodPot1I2O[I0, O0, O1, KN]

  override def input0: LSocket[I0, LMethod1I2O[I0, O0, O1]] = LSocketI0(this)

  override def output0: LSocket[O0, LMethod1I2O[I0, O0, O1]] = LSocketO0(this)

  override def output1: LSocket[O1, LMethod1I2O[I0, O0, O1]] = LSocketO1(this)

  override def plusKey[KN: TypeTag](namer: Namer): Parent[KN] =
    LMethodPot1I2O[I0, O0, O1, KN](namer.name(tag.plusKey[KN]), this)
}

