package loamstream.model.streams.methods

import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
object LMethod {

  trait Has1I[I0 <: LPileTag, MT <: LMethodTag with LMethodTag.Has1I[I0], M <: LMethod[MT]] extends LMethod[MT] {
    def input0: LSocket[I0, MT, M]
  }

  trait Has2I[I0 <: LPileTag, I1 <: LPileTag, MT <: LMethodTag with LMethodTag.Has2I[I0, I1], M <: LMethod[MT]]
    extends Has1I[I0, MT, M] {
    def input1: LSocket[I1, MT, M]
  }

  trait Has3I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, MT <: LMethodTag with LMethodTag.Has3I[I0, I1, I2],
  M <: LMethod[MT]]
    extends Has2I[I0, I1, MT, M] {
    def input2: LSocket[I2, MT, M]
  }

  trait Has1O[O0 <: LPileTag, MT <: LMethodTag with LMethodTag.Has1O[O0], M <: LMethod[MT]] extends LMethod[MT] {
    def output0: LSocket[O0, MT, M]
  }

  trait Has2O[O0 <: LPileTag, O1 <: LPileTag, MT <: LMethodTag with LMethodTag.Has2O[O0, O1], M <: LMethod[MT]]
    extends Has1O[O0, MT, M] {
    def output1: LSocket[O1, MT, M]
  }

  trait Has3O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, MT <: LMethodTag with LMethodTag.Has3O[O0, O1, O2],
  M <: LMethod[MT]]
    extends Has2O[O0, O1, MT, M] {
    def output2: LSocket[O2, MT, M]
  }

}

trait LMethod[T <: LMethodTag] {
  def tag: T
}
