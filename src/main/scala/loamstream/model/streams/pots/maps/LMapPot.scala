package loamstream.model.streams.pots.maps

import loamstream.model.streams.maps.LMap
import loamstream.model.streams.pots.piles.LPilePot

/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
trait LMapPot[V, C <: LMap[V]] extends LMap[V] with LPilePot[C] {
  def child: C
}
