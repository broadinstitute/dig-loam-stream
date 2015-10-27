package loamstream.model.collections.tags.methods

import loamstream.model.collections.tags.heaps.LHeapTag

/**
 * LoamStream
 * Created by oliverr on 10/27/2015.
 */
case class LMethodTag2I1O[I0 <: LHeapTag, I1 <: LHeapTag, O0 <: LHeapTag](input0: I0, input1: I1, output0: O0)
  extends LMethodTag with LMethodTag.Has2I[I0, I1] with LMethodTag.Has1O[O0] {
  override def inputs: Seq[LHeapTag] = Seq(input0, input1)

  override def outputs: Seq[LHeapTag] = Seq(output0)
}
