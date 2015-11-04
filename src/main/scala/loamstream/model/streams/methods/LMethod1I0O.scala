package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.Has1I
import loamstream.model.tags.methods.LMethodTag1I0O
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/29/2015.
 */
trait LMethod1I0O[I0 <: LPileTag, M <: LMethod[LMethodTag1I0O[I0]]]
  extends Has1I[I0, LMethodTag1I0O[I0], M]

