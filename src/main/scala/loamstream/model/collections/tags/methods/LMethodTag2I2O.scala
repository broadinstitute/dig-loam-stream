package loamstream.model.collections.tags.methods

import loamstream.model.collections.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/27/2015.
 */
case class LMethodTag2I2O[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag]
(input0: I0, input1: I1, output0: O0, output1: O1)
  extends LMethodTag with LMethodTag.Has2I[I0, I1] with LMethodTag.Has2O[O0, O1] {
  override def inputs: Seq[LPileTag] = Seq(input0, input1)

  override def outputs: Seq[LPileTag] = Seq(output0, output1)
}
