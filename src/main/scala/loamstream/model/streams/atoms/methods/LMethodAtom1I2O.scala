package loamstream.model.streams.atoms.methods

import loamstream.model.streams.methods.LMethod1I2O
import loamstream.model.tags.methods.LMethodTag1I2O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
case class LMethodAtom1I2O[I0 <: LPileTag : TypeTag, O0 <: LPileTag : TypeTag,
O1 <: LPileTag : TypeTag](id: String, tag: LMethodTag1I2O[I0, O0, O1])
  extends LMethodAtom with LMethod1I2O[I0, O0, O1]