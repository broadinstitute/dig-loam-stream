package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.Has1I
import loamstream.model.streams.methods.LMethod1I0O.LSocketI0
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I0O
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
object LMethod1I0O {

  case class LSocketI0[I0 <: LPileTag](method: LMethod1I0O[I0]) extends LSocket[I0, LMethod1I0O[I0]] {
    override def pileTag = method.tag.input0
  }

}

trait LMethod1I0O[I0 <: LPileTag] extends Has1I[I0, LMethod1I0O[I0]] {
  type MTag = LMethodTag1I0O[I0]

  override def input0: LSocket[I0, LMethod1I0O[I0]] = LSocketI0(this)

}

