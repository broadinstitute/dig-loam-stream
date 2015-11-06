package loamstream.model.streams.piles

import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LPile {
  type PTag <: LPileTag

  def tag: PTag
}
