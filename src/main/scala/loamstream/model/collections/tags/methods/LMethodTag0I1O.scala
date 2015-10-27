package loamstream.model.collections.tags.methods

import loamstream.model.collections.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/27/2015.
 */
case class LMethodTag0I1O[O0 <: LPileTag](output0: O0)
  extends LMethodTag with LMethodTag.Has1O[O0] {
  override def inputs: Seq[LPileTag] = Seq.empty

  override def outputs: Seq[LPileTag] = Seq(output0)
}
