package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1O, Has2I, Namer}
import loamstream.model.streams.methods.LMethod2I1O.{LSocketI0, LSocketI1, LSocketO0}
import loamstream.model.streams.pots.methods.LMethodPot2I1O
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag2I1O
import loamstream.model.tags.piles.LPileTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag
/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
object LMethod2I1O {

  case class LSocketI0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](method: LMethod2I1O[I0, I1, O0])
    extends LSocket[I0, LMethod2I1O[I0, I1, O0]] {
    override def pileTag = method.tag.input0
  }

  case class LSocketI1[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](method: LMethod2I1O[I0, I1, O0])
    extends LSocket[I1, LMethod2I1O[I0, I1, O0]] {
    override def pileTag = method.tag.input1
  }

  case class LSocketO0[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](method: LMethod2I1O[I0, I1, O0])
    extends LSocket[O0, LMethod2I1O[I0, I1, O0]] {
    override def pileTag = method.tag.output0
  }

}

trait LMethod2I1O[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag]
  extends Has2I[I0, I1, LMethod2I1O[I0, I1, O0]] with Has1O[O0, LMethod2I1O[I0, I1, O0]] {
  type MTag = LMethodTag2I1O[I0, I1, O0]
  type Parent[_] = LMethodPot2I1O[I0, I1, O0, _]

  override def input0: LSocket[I0, LMethod2I1O[I0, I1, O0]] = LSocketI0(this)

  override def input1: LSocket[I1, LMethod2I1O[I0, I1, O0]] = LSocketI1(this)

  override def output0: LSocket[O0, LMethod2I1O[I0, I1, O0]] = LSocketO0(this)

  override def plusKey[KN: TypeTag](namer: Namer): Parent[KN] =
    LMethodPot2I1O[I0, I1, O0, KN](namer.name(tag.plusKey[KN]), this)
}

