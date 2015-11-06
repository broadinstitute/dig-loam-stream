package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1O, Has2I}
import loamstream.model.tags.methods.LMethodTag2I1O
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/29/2015.
 */
trait LMethod2I1O[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag, M <: LMethod]
  extends Has2I[I0, I1, M] with Has1O[O0, M] {
  type T = LMethodTag2I1O[I0, I1, O0]
}

