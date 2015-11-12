package loamstream.model.streams.atoms.methods

import loamstream.model.streams.methods.LMethod1I0O
import loamstream.model.streams.methods.LMethod1I0O.LSocketI0
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I0O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
case class LMethodAtom1I0O[I0 <: LPileTag : TypeTag](id: String, tag: LMethodTag1I0O[I0])
  extends LMethodAtom with LMethod1I0O[I0] {

}
