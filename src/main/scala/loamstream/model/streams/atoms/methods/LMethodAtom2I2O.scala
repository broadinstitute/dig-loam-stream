package loamstream.model.streams.atoms.methods

import loamstream.model.streams.atoms.methods.LMethodAtom2I2O.{LSocketI0, LSocketI1, LSocketO0, LSocketO1}
import loamstream.model.streams.methods.LMethod2I2O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag2I2O
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 11/2/15.
  */
object LMethodAtom2I2O {

  case class LSocketI0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag,
  O1 <: LPileTag](method: LMethodAtom2I2O[I0, I1, O0, O1])
    extends LSocket[I0, LMethodAtom2I2O[I0, I1, O0, O1]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketI1[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag,
  O1 <: LPileTag](method: LMethodAtom2I2O[I0, I1, O0, O1])
    extends LSocket[I1, LMethodAtom2I2O[I0, I1, O0, O1]] {
    override def pileTag = method.tag.input1
  }

  case class LSocketO0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag,
  O1 <: LPileTag](method: LMethodAtom2I2O[I0, I1, O0, O1])
    extends LSocket[O0, LMethodAtom2I2O[I0, I1, O0, O1]] {
    override def pileTag = method.tag.output0
  }

  case class LSocketO1[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag,
  O1 <: LPileTag](method: LMethodAtom2I2O[I0, I1, O0, O1])
    extends LSocket[O1, LMethodAtom2I2O[I0, I1, O0, O1]] {
    override def pileTag = method.tag.output1
  }

}

case class LMethodAtom2I2O[I0 <: LPileTag : TypeTag, I1 <: LPileTag : TypeTag, O0 <: LPileTag : TypeTag,
O1 <: LPileTag : TypeTag](id: String, tag: LMethodTag2I2O[I0, I1, O0, O1])
  extends LMethodAtom with LMethod2I2O[I0, I1, O0, O1] {
  override def input0: LSocket[I0, LMethod2I2O[I0, I1, O0, O1]] = LSocketI0(this)

  override def input1: LSocket[I1, LMethod2I2O[I0, I1, O0, O1]] = LSocketI1(this)

  override def output0: LSocket[O0, LMethod2I2O[I0, I1, O0, O1]] = LSocketO0(this)

  override def output1: LSocket[O1, LMethod2I2O[I0, I1, O0, O1]] = LSocketO1(this)

}
