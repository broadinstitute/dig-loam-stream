package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.Has2I
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag2I0O
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
object LMethod2I0O {

  case class LSocketI0[I0 <: LPileTag, I1 <: LPileTag](method: LMethod2I0O[I0, I1])
    extends LSocket[I0, LMethod2I0O[I0, I1]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketI1[I0 <: LPileTag, I1 <: LPileTag](method: LMethod2I0O[I0, I1])
    extends LSocket[I1, LMethod2I0O[I0, I1]] {
    override def pileTag = method.tag.input1
  }

}

trait LMethod2I0O[I0 <: LPileTag, I1 <: LPileTag] extends Has2I[I0, I1, LMethod2I0O[I0, I1]] {
  type MTag = LMethodTag2I0O[I0, I1]
}

