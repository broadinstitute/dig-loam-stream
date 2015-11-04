package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.{Has1I, Has2O}
import loamstream.model.tags.methods.LMethodTag1I2O
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/29/2015.
 */
trait LMethod1I2O[I0 <: LPileTag, O0 <: LPileTag, O1 <: LPileTag, M <: LMethod[LMethodTag1I2O[I0, O0, O1]]]
  extends Has1I[I0, LMethodTag1I2O[I0, O0, O1], M] with Has2O[O0, O1, LMethodTag1I2O[I0, O0, O1], M]

