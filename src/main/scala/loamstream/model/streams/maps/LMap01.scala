package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile01
import loamstream.model.tags.maps.LMapTag01

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LMap01[K00, V] extends LMap[V] with LPile01[K00] {
  type PTag = LMapTag01[K00, V]
}
