package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1O, Has2I}
import loamstream.model.tags.methods.LMethodTag2I1O
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/29/2015.
 */
trait LMethod2I1O[I0 <: LPileTag, I1 <: LPileTag, O0 <: LPileTag]
  extends Has2I[I0, I1, LMethod2I1O[I0, I1, O0]] with Has1O[O0, LMethod2I1O[I0, I1, O0]] {
  type MTag = LMethodTag2I1O[I0, I1, O0]
}

