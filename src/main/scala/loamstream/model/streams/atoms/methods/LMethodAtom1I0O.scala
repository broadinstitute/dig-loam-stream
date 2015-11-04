package loamstream.model.streams.atoms.methods

import loamstream.model.streams.atoms.methods.LMethodAtom1I0O.LSocketI0
import loamstream.model.streams.methods.LMethod1I0O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I0O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 11/2/15.
 */
object LMethodAtom1I0O {

  case class LSocketI0[I0 <: LPileTag](method: LMethodAtom1I0O[I0])
    extends LSocket[I0, LMethodTag1I0O[I0], LMethodAtom1I0O[I0]] {
    override def pileTag = method.tag.input0
  }

}

case class LMethodAtom1I0O[I0 <: LPileTag : TypeTag](id: String, tag: LMethodTag1I0O[I0])
  extends LMethod1I0O[I0, LMethodAtom1I0O[I0]] {
  override def input0: LSocket[I0, LMethodTag1I0O[I0], LMethodAtom1I0O[I0]] = LSocketI0(this)

}
