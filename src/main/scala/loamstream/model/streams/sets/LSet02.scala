package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.piles.LPile02
import loamstream.model.streams.pots.sets.LSetPot03
import loamstream.model.tags.sets.LSetTag02

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet02[K00, K01] extends LSet with LPile02[K00, K01] {
  type PTag = LSetTag02[K00, K01]
  type Parent[_] = LSetPot03[K00, K01, _]

  def plusKey[KN: TypeTag](namer: Namer) = LSetPot03.create[K00, K01, KN](namer.name(tag.plusKey[KN]), this)
}
