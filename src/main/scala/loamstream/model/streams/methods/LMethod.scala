package loamstream.model.streams.methods

import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
object LMethod {

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

trait LMethod {
  type MTag <: LMethodTag

  def tag: MTag
}
