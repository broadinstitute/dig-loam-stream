package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile
import loamstream.model.streams.pots.sets.LSetPot
import loamstream.model.tags.sets.LSetTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet extends LPile {
  type PTag <: LSetTag

  def addKey[K]: LSetPot[_]

}
