package loamstream.model.tags.methods

import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/27/2015.
  */
case class LMethodTag2I2O[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag]
(input0: I0, input1: I1, output0: O0, output1: O1)
  extends LMethodTag with LMethodTag.Has2I[I0, I1] with LMethodTag.Has2O[O0, O1] {
  override def inputs: Seq[LPileTag] = Seq(input0, input1)

  override def outputs: Seq[LPileTag] = Seq(output0, output1)

  def plusKeyI0[KN: TypeTag] =
    LMethodTag2I2O[I0#UpTag[KN], I1, O0#UpTag[KN], O1#UpTag[KN]](input0.plusKey[KN], input1, output0.plusKey[KN],
      output1.plusKey[KN])

  def plusKeyI1[KN: TypeTag] =
    LMethodTag2I2O[I0, I1#UpTag[KN], O0#UpTag[KN], O1#UpTag[KN]](input0, input1.plusKey[KN], output0.plusKey[KN],
      output1.plusKey[KN])
}
