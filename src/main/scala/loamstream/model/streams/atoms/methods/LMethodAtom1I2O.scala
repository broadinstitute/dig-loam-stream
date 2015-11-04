package loamstream.model.streams.atoms.methods

import loamstream.model.streams.atoms.methods.LMethodAtom1I2O.{LSocketO1, LSocketI0, LSocketO0}
import loamstream.model.streams.methods.LMethod1I2O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag1I2O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 11/2/15.
 */
object LMethodAtom1I2O {

  case class LSocketI0[I0 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag](method: LMethodAtom1I2O[I0, O0, O1])
    extends LSocket[I0, LMethodTag1I2O[I0, O0, O1], LMethodAtom1I2O[I0, O0, O1]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketO0[I0 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag](method: LMethodAtom1I2O[I0, O0, O1])
    extends LSocket[O0, LMethodTag1I2O[I0, O0, O1], LMethodAtom1I2O[I0, O0, O1]] {
    override def pileTag = method.tag.output0
  }

  case class LSocketO1[I0 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag](method: LMethodAtom1I2O[I0, O0, O1])
    extends LSocket[O1, LMethodTag1I2O[I0, O0, O1], LMethodAtom1I2O[I0, O0, O1]] {
    override def pileTag = method.tag.output1
  }

}

case class LMethodAtom1I2O[I0 <: LPileTag : TypeTag, O0 <: LPileTag : TypeTag,
O1 <: LPileTag : TypeTag](id: String, tag: LMethodTag1I2O[I0, O0, O1])
  extends LMethod1I2O[I0, O0, O1, LMethodAtom1I2O[I0, O0, O1]] {
  override def input0: LSocket[I0, LMethodTag1I2O[I0, O0, O1], LMethodAtom1I2O[I0, O0, O1]] = LSocketI0(this)

  override def output0: LSocket[O0, LMethodTag1I2O[I0, O0, O1], LMethodAtom1I2O[I0, O0, O1]] = LSocketO0(this)

  override def output1: LSocket[O1, LMethodTag1I2O[I0, O0, O1], LMethodAtom1I2O[I0, O0, O1]] = LSocketO1(this)

}
