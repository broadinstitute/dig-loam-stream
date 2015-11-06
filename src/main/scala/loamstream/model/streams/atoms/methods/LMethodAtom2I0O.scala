package loamstream.model.streams.atoms.methods

import loamstream.model.streams.atoms.methods.LMethodAtom2I0O.{LSocketI0, LSocketI1}
import loamstream.model.streams.methods.LMethod2I0O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag2I0O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
object LMethodAtom2I0O {

  case class LSocketI0[I0 <: LPileTag, I1 <: LPileTag](method: LMethodAtom2I0O[I0, I1])
    extends LSocket[I0, LMethodAtom2I0O[I0, I1]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketI1[I0 <: LPileTag, I1 <: LPileTag](method: LMethodAtom2I0O[I0, I1])
    extends LSocket[I1, LMethodAtom2I0O[I0, I1]] {
    override def pileTag = method.tag.input1
  }

}

case class LMethodAtom2I0O[I0 <: LPileTag : TypeTag,
I1 <: LPileTag : TypeTag](id: String, tag: LMethodTag2I0O[I0, I1])
  extends LMethodAtom with LMethod2I0O[I0, I1, LMethodAtom2I0O[I0, I1]] {
  override def input0: LSocket[I0, LMethodAtom2I0O[I0, I1]] = LSocketI0(this)

  override def input1: LSocket[I1, LMethodAtom2I0O[I0, I1]] = LSocketI1(this)

}
