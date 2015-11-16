package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.piles.LPile02
import loamstream.model.streams.pots.maps.LMapPot03
import loamstream.model.tags.maps.LMapTag02

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LMap02[K00, K01, V] extends LMap[V] with LPile02[K00, K01] {
  type PTag = LMapTag02[K00, K01, V]
  type Parent[KN] = LMapPot03[K00, K01, KN, V]

  def plusKey[KN: TypeTag](namer: Namer): Parent[KN] = LMapPot03.create(namer.name(tag.plusKey[KN]), this)
}
