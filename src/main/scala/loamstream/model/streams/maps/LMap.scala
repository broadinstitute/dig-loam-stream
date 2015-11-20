package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile
import loamstream.model.streams.pots.maps.LMapPot
import loamstream.model.tags.maps.LMapTag

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LMap[V] extends LPile {
  type PTag <: LMapTag[V]
  type Parent[KN] <: LMapPot[V, LMap[V]]
}
