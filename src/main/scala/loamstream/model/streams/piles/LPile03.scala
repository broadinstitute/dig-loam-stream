package loamstream.model.streams.piles

import loamstream.model.tags.piles.LPileTag03

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LPile03[K00, K01, K02] extends LPile {
  type PTag <: LPileTag03[K00, K01, K02]
}
