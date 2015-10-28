package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile02
import loamstream.model.tags.maps.LMapTag02

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
trait LMap02[K00, K01, V] extends LMap[V, LMapTag02[K00, K01, V]] with LPile02[K00, K01, LMapTag02[K00, K01, V]] {

}
