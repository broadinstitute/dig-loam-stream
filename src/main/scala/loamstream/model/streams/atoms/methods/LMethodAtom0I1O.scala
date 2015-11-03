package loamstream.model.streams.atoms.methods

import loamstream.model.streams.atoms.methods.LMethodAtom0I1O.LSocketO0
import loamstream.model.streams.methods.LMethod0I1O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag0I1O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 11/2/15.
 */
object LMethodAtom0I1O {

  case class LSocketO0[O0 <: LPileTag](method: LMethodAtom0I1O[O0])
    extends LSocket[O0, LMethodTag0I1O[O0], LMethodAtom0I1O[O0]] {
    override def pileTag = method.tag.output0
  }

}

case class LMethodAtom0I1O[O0 <: LPileTag : TypeTag](id: String, tag: LMethodTag0I1O[O0])
  extends LMethod0I1O[O0, LMethodTag0I1O[O0], LMethodAtom0I1O[O0]] {
  override def output0: LSocket[O0, LMethodTag0I1O[O0], LMethodAtom0I1O[O0]] = LSocketO0(this)

}
