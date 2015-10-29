package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.Has1O
import loamstream.model.tags.methods.LMethodTag0I1O
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/29/2015.
 */
trait LMethod0I1O[O0 <: LPileTag, MT <: LMethodTag0I1O[O0], M <: LMethod[MT]]
  extends Has1O[O0, MT, M]

