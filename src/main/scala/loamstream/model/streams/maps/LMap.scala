package loamstream.model.streams.maps

import loamstream.model.streams.piles.LPile
import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.pots.maps.LMapPot
import loamstream.model.tags.maps.LMapTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LMap[V] extends LPile {
  type PTag <: LMapTag[V]

  def addKey[K](namer: Namer): LMapPot[V, _]

}
