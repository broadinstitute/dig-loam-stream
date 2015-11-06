package loamstream.model.streams.piles

import loamstream.model.tags.piles.LPileTag02

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LPile02[K00, K01] extends LPile {
  type T <: LPileTag02[K00, K01]
}
