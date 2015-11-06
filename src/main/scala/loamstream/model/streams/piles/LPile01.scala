package loamstream.model.streams.piles

import loamstream.model.tags.piles.LPileTag01

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LPile01[K00] extends LPile {
  type T <: LPileTag01[K00]
}
