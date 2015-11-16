package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.piles.LPile01
import loamstream.model.streams.pots.maps.LMapPot02
import loamstream.model.tags.maps.LMapTag01

import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LMap01[K00, V] extends LMap[V] with LPile01[K00] {
  type PTag = LMapTag01[K00, V]
  type Parent[KN] = LMapPot02[K00, KN, V]

  def plusKey[KN: TypeTag](namer: Namer): Parent[KN] = LMapPot02.create(namer.name(tag.plusKey[KN]), this)
}
