package loamstream.model.tags.methods

import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/27/2015.
 */
case class LMethodTag1I2O[I0 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag](input0: I0, output0: O0, output1: O1)
  extends LMethodTag with LMethodTag.Has1I[I0] with LMethodTag.Has2O[O0, O1] {
  override def inputs: Seq[LPileTag] = Seq(input0)

  override def outputs: Seq[LPileTag] = Seq(output0, output1)
}
