package loamstream.model.collections.tags.methods

import loamstream.model.collections.tags.heaps.LHeapTag

/**
 * LoamStream
 * Created by oliverr on 10/22/2015.
 */
object LMethodTag {

  trait Has1I[I0 <: LHeapTag] {
    def input0: I0
  }

  trait Has2I[I0 <: LHeapTag, I1 <: LHeapTag] extends Has1I[I0] {
    def input1: I1
  }

  trait Has3I[I0 <: LHeapTag, I1 <: LHeapTag, I2 <: LHeapTag] extends Has2I[I0, I1] {
    def input2: I2
  }

  trait Has4I[I0 <: LHeapTag, I1 <: LHeapTag, I2 <: LHeapTag, I3 <: LHeapTag] extends Has3I[I0, I1, I2] {
    def input3: I3
  }

  trait Has5I[I0 <: LHeapTag, I1 <: LHeapTag, I2 <: LHeapTag, I3 <: LHeapTag, I4 <: LHeapTag]
    extends Has4I[I0, I1, I2, I3] {
    def input4: I4
  }

  trait Has6I[I0 <: LHeapTag, I1 <: LHeapTag, I2 <: LHeapTag, I3 <: LHeapTag, I4 <: LHeapTag, I5 <: LHeapTag]
    extends Has5I[I0, I1, I2, I3, I4] {
    def input5: I5
  }

  trait Has7I[I0 <: LHeapTag, I1 <: LHeapTag, I2 <: LHeapTag, I3 <: LHeapTag, I4 <: LHeapTag, I5 <: LHeapTag,
  I6 <: LHeapTag]
    extends Has6I[I0, I1, I2, I3, I4, I5] {
    def input6: I6
  }

  trait Has1O[O0 <: LHeapTag] {
    def output0: O0
  }

  trait Has2O[O0 <: LHeapTag, O1 <: LHeapTag] extends Has1O[O0] {
    def output1: O1
  }

  trait Has3O[O0 <: LHeapTag, O1 <: LHeapTag, O2 <: LHeapTag] extends Has2O[O0, O1] {
    def output2: O2
  }

  trait Has4O[O0 <: LHeapTag, O1 <: LHeapTag, O2 <: LHeapTag, O3 <: LHeapTag] extends Has3O[O0, O1, O2] {
    def output3: O3
  }

  trait Has5O[O0 <: LHeapTag, O1 <: LHeapTag, O2 <: LHeapTag, O3 <: LHeapTag, O4 <: LHeapTag]
    extends Has4O[O0, O1, O2, O3] {
    def output4: O4
  }

  trait Has6O[O0 <: LHeapTag, O1 <: LHeapTag, O2 <: LHeapTag, O3 <: LHeapTag, O4 <: LHeapTag, O5 <: LHeapTag]
    extends Has5O[O0, O1, O2, O3, O4] {
    def output5: O5
  }

  trait Has7O[O0 <: LHeapTag, O1 <: LHeapTag, O2 <: LHeapTag, O3 <: LHeapTag, O4 <: LHeapTag, O5 <: LHeapTag,
  O6 <: LHeapTag]
    extends Has6O[O0, O1, O2, O3, O4, O5] {
    def output5: O5
  }

}

trait LMethodTag {

  def inputs: Seq[LHeapTag]

  def outputs: Seq[LHeapTag]

}
