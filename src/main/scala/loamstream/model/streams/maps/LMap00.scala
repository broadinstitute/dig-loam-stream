package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile00
import loamstream.model.tags.maps.LMapTag00

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
trait LMap00[V] extends LMap[V, LMapTag00[V]] with LPile00[LMapTag00[V]] {

}
