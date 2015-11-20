package loamstream.model.tags.methods

import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/27/2015.
  */
case class LMethodTag2I1O[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag](input0: I0, input1: I1, output0: O0)
  extends LMethodTag with LMethodTag.Has2I[I0, I1] with LMethodTag.Has1O[O0] {
  override def inputs: Seq[LPileTag] = Seq(input0, input1)

  override def outputs: Seq[LPileTag] = Seq(output0)

  def plusKey[KN: TypeTag] =
    LMethodTag2I1O[I0#UpTag[KN], I1#UpTag[KN], O0#UpTag[KN]](input0.plusKey[KN], input1.plusKey[KN],
      output0.plusKey[KN])

  def plusKeyI0[KN: TypeTag] =
    LMethodTag2I1O[I0#UpTag[KN], I1, O0#UpTag[KN]](input0.plusKey[KN], input1, output0.plusKey[KN])

  def plusKeyI1[KN: TypeTag] =
    LMethodTag2I1O[I0, I1#UpTag[KN], O0#UpTag[KN]](input0, input1.plusKey[KN], output0.plusKey[KN])
}
