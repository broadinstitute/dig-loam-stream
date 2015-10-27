package loamstream.model.collections.tags.methods

import loamstream.model.collections.tags.heaps.LHeapTag

/**
 * LoamStream
 * Created by oliverr on 10/27/2015.
 */
case class LMethodTag0I1O[O0 <: LHeapTag](output0: O0)
  extends LMethodTag with LMethodTag.Has1O[O0] {
  override def inputs: Seq[LHeapTag] = Seq.empty

  override def outputs: Seq[LHeapTag] = Seq(output0)
}
