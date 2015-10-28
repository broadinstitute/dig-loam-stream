package loamstream.model.tags.methods

import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/27/2015.
 */
case class LMethodTag1I0O[I0 <: LPileTag](input0: I0) extends LMethodTag with LMethodTag.Has1I[I0] {
  override def inputs: Seq[LPileTag] = Seq(input0)

  override def outputs: Seq[LPileTag] = Seq.empty
}
