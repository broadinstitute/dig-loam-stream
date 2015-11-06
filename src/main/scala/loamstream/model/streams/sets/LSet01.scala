package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile01
import loamstream.model.tags.sets.LSetTag01

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet01[K00] extends LSet with LPile01[K00] {
  type PTag = LSetTag01[K00]
}
