package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.piles.LPile00
import loamstream.model.streams.pots.maps.LMapPot01
import loamstream.model.tags.maps.LMapTag00

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LMap00[V] extends LMap[V] with LPile00 {
  type PTag = LMapTag00[V]
  type Parent[_] = LMapPot01[_, V]

  def plusKey[KN: TypeTag](namer: Namer): Parent[_] = {
    LMapPot01.create[KN, V](namer.name(tag.plusKey), this)
  }

}
