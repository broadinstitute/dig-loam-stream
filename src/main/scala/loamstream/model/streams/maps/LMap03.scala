package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile03
import loamstream.model.tags.maps.LMapTag03

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
trait LMap03[K00, K01, K02, V]
  extends LMap[V, LMapTag03[K00, K01, K02, V]] with LPile03[K00, K01, K02, LMapTag03[K00, K01, K02, V]] {

}
