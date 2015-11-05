package loamstream.model.streams.atoms.methods

import loamstream.model.streams.atoms.methods.LMethodAtom2I1O.{LSocketI0, LSocketI1, LSocketO0}
import loamstream.model.streams.methods.LMethod2I1O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag2I1O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
object LMethodAtom2I1O {

  case class LSocketI0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](method: LMethodAtom2I1O[I0, I1, O0])
    extends LSocket[I0, LMethodTag2I1O[I0, I1, O0], LMethodAtom2I1O[I0, I1, O0]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketI1[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](method: LMethodAtom2I1O[I0, I1, O0])
    extends LSocket[I1, LMethodTag2I1O[I0, I1, O0], LMethodAtom2I1O[I0, I1, O0]] {
    override def pileTag = method.tag.input1
  }

  case class LSocketO0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](method: LMethodAtom2I1O[I0, I1, O0])
    extends LSocket[O0, LMethodTag2I1O[I0, I1, O0], LMethodAtom2I1O[I0, I1, O0]] {
    override def pileTag = method.tag.output0
  }

}

case class LMethodAtom2I1O[I0 <: LPileTag : TypeTag, I1 <: LPileTag : TypeTag,
O0 <: LPileTag : TypeTag](id: String, tag: LMethodTag2I1O[I0, I1, O0])
  extends LMethodAtom with LMethod2I1O[I0, I1, O0, LMethodAtom2I1O[I0, I1, O0]] {
  override def input0: LSocket[I0, LMethodTag2I1O[I0, I1, O0], LMethodAtom2I1O[I0, I1, O0]] = LSocketI0(this)

  override def input1: LSocket[I1, LMethodTag2I1O[I0, I1, O0], LMethodAtom2I1O[I0, I1, O0]] = LSocketI1(this)

  override def output0: LSocket[O0, LMethodTag2I1O[I0, I1, O0], LMethodAtom2I1O[I0, I1, O0]] = LSocketO0(this)

}
