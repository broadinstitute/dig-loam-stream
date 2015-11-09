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

  def addKey[K: TypeTag](namer: Namer): LMapPot01[K, V] = {
    LMapPot01.create[K, V](namer.name(tag.plusKey), this)
  }

}
