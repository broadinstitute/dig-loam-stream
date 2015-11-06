package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile03
import loamstream.model.tags.sets.LSetTag03

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet03[K00, K01, K02]
  extends LSet with LPile03[K00, K01, K02] {
  type PTag = LSetTag03[K00, K01, K02]
}
