package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1I, Namer}
import loamstream.model.streams.methods.LMethod1I0O.LSocketI0
import loamstream.model.streams.pots.methods.LMethodPot1I0O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I0O
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

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
  type Parent[KN] = LMethodPot1I0O[I0, KN]

  override def input0: LSocket[I0, LMethod1I0O[I0]] = LSocketI0(this)

  override def plusKey[KN: TypeTag](namer: Namer): Parent[KN] =
    LMethodPot1I0O[I0, KN](namer.name(tag.plusKey[KN]), this)
}

