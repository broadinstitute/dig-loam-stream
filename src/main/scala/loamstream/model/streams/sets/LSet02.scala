package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile02
import loamstream.model.tags.sets.LSetTag02

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet02[K00, K01] extends LSet with LPile02[K00, K01] {
  type T = LSetTag02[K00, K01]
}
