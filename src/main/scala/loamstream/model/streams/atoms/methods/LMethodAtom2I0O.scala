package loamstream.model.streams.atoms.methods

import loamstream.model.streams.methods.LMethod2I0O
import loamstream.model.tags.methods.LMethodTag2I0O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
case class LMethodAtom2I0O[I0 <: LPileTag : TypeTag,
I1 <: LPileTag : TypeTag](id: String, tag: LMethodTag2I0O[I0, I1])
  extends LMethodAtom with LMethod2I0O[I0, I1]