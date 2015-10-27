package loamstream.model.collections.tags.methods

import loamstream.model.collections.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/27/2015.
 */
case class LMethodTag0I2O[O0 <: LPileTag, O1 <: LPileTag](output0: O0, output1: O1)
  extends LMethodTag with LMethodTag.Has2O[O0, O1] {
  override def inputs: Seq[LPileTag] = Seq.empty

  override def outputs: Seq[LPileTag] = Seq(output0, output1)
}
