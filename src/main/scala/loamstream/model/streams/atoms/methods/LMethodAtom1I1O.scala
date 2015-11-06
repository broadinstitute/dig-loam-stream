package loamstream.model.streams.atoms.methods

import loamstream.model.streams.atoms.methods.LMethodAtom1I1O.{LSocketI0, LSocketO0}
import loamstream.model.streams.methods.LMethod1I1O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I1O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
object LMethodAtom1I1O {

  case class LSocketI0[I0 <: LPileTag, O0 <: LPileTag](method: LMethodAtom1I1O[I0, O0])
    extends LSocket[I0, LMethodAtom1I1O[I0, O0]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketO0[I0 <: LPileTag, O0 <: LPileTag](method: LMethodAtom1I1O[I0, O0])
    extends LSocket[O0, LMethodAtom1I1O[I0, O0]] {
    override def pileTag = method.tag.output0
  }

}

case class LMethodAtom1I1O[I0 <: LPileTag : TypeTag,
O0 <: LPileTag : TypeTag](id: String, tag: LMethodTag1I1O[I0, O0])
  extends LMethodAtom with LMethod1I1O[I0, O0, LMethodAtom1I1O[I0, O0]] {
  override def input0: LSocket[I0, LMethodAtom1I1O[I0, O0]] = LSocketI0(this)

  override def output0: LSocket[O0, LMethodAtom1I1O[I0, O0]] = LSocketO0(this)

}
