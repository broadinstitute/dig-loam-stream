package loamstream.model.streams.atoms.methods

import loamstream.model.streams.methods.LMethod0I1O
import loamstream.model.streams.methods.LMethod0I1O.LSocketO0
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag0I1O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
case class LMethodAtom0I1O[O0 <: LPileTag : TypeTag](id: String, tag: LMethodTag0I1O[O0])
  extends LMethodAtom with LMethod0I1O[O0] {

}
