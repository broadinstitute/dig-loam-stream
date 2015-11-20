package loamstream.model.streams.atoms.methods

import loamstream.model.streams.methods.LMethod2I2O
import loamstream.model.tags.methods.LMethodTag2I2O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
case class LMethodAtom2I2O[I0 <: LPileTag : TypeTag, I1 <: LPileTag : TypeTag, O0 <: LPileTag : TypeTag,
O1 <: LPileTag : TypeTag](id: String, tag: LMethodTag2I2O[I0, I1, O0, O1])
  extends LMethodAtom with LMethod2I2O[I0, I1, O0, O1]