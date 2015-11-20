package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has2O, Namer}
import loamstream.model.streams.methods.LMethod0I2O.{LSocketO0, LSocketO1}
import loamstream.model.streams.pots.methods.LMethodPot0I2O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag0I2O
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
object LMethod0I2O {

  case class LSocketO0[O0 <: LPileTag, O1 <: LPileTag](method: LMethod0I2O[O0, O1])
    extends LSocket[O0, LMethod0I2O[O0, O1]] {
    override def pileTag = method.tag.output0
  }

  case class LSocketO1[O0 <: LPileTag, O1 <: LPileTag](method: LMethod0I2O[O0, O1])
    extends LSocket[O1, LMethod0I2O[O0, O1]] {
    override def pileTag = method.tag.output1
  }

}


trait LMethod0I2O[O0 <: LPileTag, O1 <: LPileTag] extends Has2O[O0, O1, LMethod0I2O[O0, O1]] {
  type MTag = LMethodTag0I2O[O0, O1]
  type Parent[KN] = LMethodPot0I2O[O0, O1, KN]

  override def output0: LSocket[O0, LMethod0I2O[O0, O1]] = LSocketO0(this)

  override def output1: LSocket[O1, LMethod0I2O[O0, O1]] = LSocketO1(this)

  override def plusKey[KN: TypeTag](namer: Namer): Parent[KN] =
    LMethodPot0I2O[O0, O1, KN](namer.name(tag.plusKey[KN]), this)
}

