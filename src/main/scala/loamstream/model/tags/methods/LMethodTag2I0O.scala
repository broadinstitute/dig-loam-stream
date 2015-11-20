package loamstream.model.tags.methods

import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/27/2015.
  */
case class LMethodTag2I0O[I0 <: LPileTag, I1 <: LPileTag](input0: I0, input1: I1)
  extends LMethodTag with LMethodTag.Has2I[I0, I1] {
  override def inputs: Seq[LPileTag] = Seq(input0, input1)

  override def outputs: Seq[LPileTag] = Seq.empty

  def plusKey[KN: TypeTag] = LMethodTag2I0O[I0#UpTag[KN], I1#UpTag[KN]](input0.plusKey[KN], input1.plusKey[KN])

  def plusKeyI0[KN: TypeTag] = LMethodTag2I0O[I0#UpTag[KN], I1](input0.plusKey[KN], input1)

  def plusKeyI1[KN: TypeTag] = LMethodTag2I0O[I0, I1#UpTag[KN]](input0, input1.plusKey[KN])
}
