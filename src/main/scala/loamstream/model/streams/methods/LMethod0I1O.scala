package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.Has1O
import loamstream.model.streams.methods.LMethod0I1O.LSocketO0
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag0I1O
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
object LMethod0I1O {

  case class LSocketO0[O0 <: LPileTag](method: LMethod0I1O[O0]) extends LSocket[O0, LMethod0I1O[O0]] {
    override def pileTag = method.tag.output0
  }

}

trait LMethod0I1O[O0 <: LPileTag] extends Has1O[O0, LMethod0I1O[O0]] {
  type MTag = LMethodTag0I1O[O0]

  override def output0: LSocket[O0, LMethod0I1O[O0]] = LSocketO0(this)
}

