package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1I, Has1O}
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I1O
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
object LMethod1I1O {

  case class LSocketI0[I0 <: LPileTag, O0 <: LPileTag](method: LMethod1I1O[I0, O0])
    extends LSocket[I0, LMethod1I1O[I0, O0]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketO0[I0 <: LPileTag, O0 <: LPileTag](method: LMethod1I1O[I0, O0])
    extends LSocket[O0, LMethod1I1O[I0, O0]] {
    override def pileTag = method.tag.output0
  }

}

trait LMethod1I1O[I0 <: LPileTag, O0 <: LPileTag]
  extends Has1I[I0, LMethod1I1O[I0, O0]] with Has1O[O0, LMethod1I1O[I0, O0]] {
  type MTag = LMethodTag1I1O[I0, O0]
}

