package loamstream.model.streams.methods

import loamstream.model.streams.LNode
import loamstream.model.streams.methods.LMethod.Namer
import loamstream.model.streams.pots.methods.LMethodPot
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag
import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
object LMethod {

  object Namer {

    object Default extends Namer {
      var i = 0

      override def name(tag: LMethodTag): String = {
        val baseName = "method"
        baseName + i
      }
    }

  }

  trait Namer {
    def name(tag: LMethodTag): String
  }


  trait Has1I[I0 <: LPileTag, M <: LMethod] extends LMethod {
    def input0: LSocket[I0, M]
  }

  trait Has2I[I0 <: LPileTag, I1 <: LPileTag, M <: LMethod] extends Has1I[I0, M] {
    def input1: LSocket[I1, M]
  }

  trait Has3I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, M <: LMethod] extends Has2I[I0, I1, M] {
    def input2: LSocket[I2, M]
  }

  trait Has1O[O0 <: LPileTag, M <: LMethod] extends LMethod {
    def output0: LSocket[O0, M]
  }

  trait Has2O[O0 <: LPileTag, O1 <: LPileTag, M <: LMethod] extends Has1O[O0, M] {
    def output1: LSocket[O1, M]
  }

  trait Has3O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, M <: LMethod] extends Has2O[O0, O1, M] {
    def output2: LSocket[O2, M]
  }

}

trait LMethod extends LNode {
  type Tag = MTag
  type MTag <: LMethodTag
  type Parent[KN] <: LMethodPot[LMethod]

  def tag: MTag

  def plusKey[KN: TypeTag](namer: Namer): Parent[KN]

  def plusKey[KN: TypeTag]: Parent[KN] = plusKey[KN](Namer.Default)
}
