package loamstream.model.streams.atoms.methods

import loamstream.model.streams.atoms.methods.LMethodAtom0I2O.{LSocketO0, LSocketO1}
import loamstream.model.streams.methods.LMethod0I2O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag0I2O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
object LMethodAtom0I2O {

  case class LSocketO0[O0 <: LPileTag, O1 <: LPileTag](method: LMethodAtom0I2O[O0, O1])
    extends LSocket[O0, LMethodAtom0I2O[O0, O1]] {
    override def pileTag = method.tag.output0
  }

  case class LSocketO1[O0 <: LPileTag, O1 <: LPileTag](method: LMethodAtom0I2O[O0, O1])
    extends LSocket[O1, LMethodAtom0I2O[O0, O1]] {
    override def pileTag = method.tag.output1
  }

}

case class LMethodAtom0I2O[O0 <: LPileTag : TypeTag,
O1 <: LPileTag : TypeTag](id: String, tag: LMethodTag0I2O[O0, O1])
  extends LMethodAtom with LMethod0I2O[O0, O1] {
  override def output0: LSocket[O0, LMethod0I2O[O0, O1]] = LSocketO0(this)

  override def output1: LSocket[O1, LMethod0I2O[O0, O1]] = LSocketO1(this)

}
