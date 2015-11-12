package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1I, Has2O}
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I2O
import loamstream.model.tags.piles.LPileTag

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
}

