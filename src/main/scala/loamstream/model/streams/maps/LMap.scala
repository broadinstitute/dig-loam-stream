package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile
import loamstream.model.tags.maps.LMapTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LMap[V] extends LPile {
  type T <: LMapTag[V]
}
