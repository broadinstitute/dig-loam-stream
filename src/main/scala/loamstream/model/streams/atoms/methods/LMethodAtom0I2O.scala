package loamstream.model.streams.atoms.methods

import loamstream.model.streams.methods.LMethod0I2O.{LSocketO0, LSocketO1}
import loamstream.model.streams.methods.LMethod0I2O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag0I2O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
case class LMethodAtom0I2O[O0 <: LPileTag : TypeTag,
O1 <: LPileTag : TypeTag](id: String, tag: LMethodTag0I2O[O0, O1])
  extends LMethodAtom with LMethod0I2O[O0, O1] {
  override def output0: LSocket[O0, LMethod0I2O[O0, O1]] = LSocketO0(this)

  override def output1: LSocket[O1, LMethod0I2O[O0, O1]] = LSocketO1(this)

}
