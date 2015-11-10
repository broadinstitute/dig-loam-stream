package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.piles.LPile03
import loamstream.model.tags.maps.LMapTag03

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LMap03[K00, K01, K02, V] extends LMap[V] with LPile03[K00, K01, K02] {
  type PTag = LMapTag03[K00, K01, K02, V]
  //  type Parent[_] = LMapPot04[K00, K01, K02, _, V] // once we have added LMapPot04
  type Parent[_] = Nothing // TODO: replace by LMapPot04[K00, K01, K02, _, V]

  //  def addKey[KN: TypeTag](namer: Namer): Parent[KN] = LMapPot04.create(namer.name(tag.plusKey[KN]), this)
  def plusKey[KN: TypeTag](namer: Namer): Parent[KN] = ??? // TODO replace by LMapPot04.create(...)

}
