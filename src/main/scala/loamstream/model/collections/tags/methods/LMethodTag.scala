package loamstream.model.collections.tags.methods

import loamstream.model.collections.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/22/2015.
 */
object LMethodTag {

  trait Has1I[I0 <: LPileTag] {
    def input0: I0
  }

  trait Has2I[I0 <: LPileTag, I1 <: LPileTag] extends Has1I[I0] {
    def input1: I1
  }

  trait Has3I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag] extends Has2I[I0, I1] {
    def input2: I2
  }

  trait Has4I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, I3 <: LPileTag] extends Has3I[I0, I1, I2] {
    def input3: I3
  }

  trait Has5I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, I3 <: LPileTag, I4 <: LPileTag]
    extends Has4I[I0, I1, I2, I3] {
    def input4: I4
  }

  trait Has6I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, I3 <: LPileTag, I4 <: LPileTag, I5 <: LPileTag]
    extends Has5I[I0, I1, I2, I3, I4] {
    def input5: I5
  }

  trait Has7I[I0 <: LPileTag, I1 <: LPileTag, I2 <: LPileTag, I3 <: LPileTag, I4 <: LPileTag, I5 <: LPileTag,
  I6 <: LPileTag]
    extends Has6I[I0, I1, I2, I3, I4, I5] {
    def input6: I6
  }

  trait Has1O[O0 <: LPileTag] {
    def output0: O0
  }

  trait Has2O[O0 <: LPileTag, O1 <: LPileTag] extends Has1O[O0] {
    def output1: O1
  }

  trait Has3O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag] extends Has2O[O0, O1] {
    def output2: O2
  }

  trait Has4O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, O3 <: LPileTag] extends Has3O[O0, O1, O2] {
    def output3: O3
  }

  trait Has5O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, O3 <: LPileTag, O4 <: LPileTag]
    extends Has4O[O0, O1, O2, O3] {
    def output4: O4
  }

  trait Has6O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, O3 <: LPileTag, O4 <: LPileTag, O5 <: LPileTag]
    extends Has5O[O0, O1, O2, O3, O4] {
    def output5: O5
  }

  trait Has7O[O0 <: LPileTag, O1 <: LPileTag, O2 <: LPileTag, O3 <: LPileTag, O4 <: LPileTag, O5 <: LPileTag,
  O6 <: LPileTag]
    extends Has6O[O0, O1, O2, O3, O4, O5] {
    def output5: O5
  }

}

trait LMethodTag {

  def inputs: Seq[LPileTag]

  def outputs: Seq[LPileTag]

}
