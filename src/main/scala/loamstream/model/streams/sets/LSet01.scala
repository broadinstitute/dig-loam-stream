package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.piles.LPile01
import loamstream.model.streams.pots.sets.LSetPot02
import loamstream.model.tags.sets.LSetTag01

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet01[K00] extends LSet with LPile01[K00] {
  type PTag = LSetTag01[K00]
  type Parent[_] = LSetPot02[K00, _]

  def plusKey[KN: TypeTag](namer: Namer) = LSetPot02.create[K00, KN](namer.name(tag.plusKey[KN]), this)

}
