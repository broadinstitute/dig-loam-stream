package loamstream.model.collections.tags.methods

import loamstream.model.collections.tags.heaps.LHeapTag

/**
 * LoamStream
 * Created by oliverr on 10/27/2015.
 */
case class LMethodTag1I1O[I0 <: LHeapTag, O0 <: LHeapTag](input0: I0, output0: O0)
  extends LMethodTag with LMethodTag.Has1I[I0] with LMethodTag.Has1O[O0] {
  override def inputs: Seq[LHeapTag] = Seq(input0)

  override def outputs: Seq[LHeapTag] = Seq(output0)
}
