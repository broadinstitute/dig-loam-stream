package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.Has2O
import loamstream.model.tags.methods.LMethodTag0I2O
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/29/2015.
 */
trait LMethod0I2O[O0 <: LPileTag, O1 <: LPileTag, M <: LMethod] extends Has2O[O0, O1, M] {
  type T = LMethodTag0I2O[O0, O1]
}

