package loamstream.model.streams.atoms.methods

import loamstream.model.streams.methods.LMethod1I1O
import loamstream.model.streams.methods.LMethod1I1O.{LSocketI0, LSocketO0}
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I1O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
case class LMethodAtom1I1O[I0 <: LPileTag : TypeTag,
O0 <: LPileTag : TypeTag](id: String, tag: LMethodTag1I1O[I0, O0])
  extends LMethodAtom with LMethod1I1O[I0, O0] {
  override def input0: LSocket[I0, LMethod1I1O[I0, O0]] = LSocketI0(this)

  override def output0: LSocket[O0, LMethod1I1O[I0, O0]] = LSocketO0(this)

}
