package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.piles.LPile03
import loamstream.model.tags.sets.LSetTag03

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet03[K00, K01, K02]
  extends LSet with LPile03[K00, K01, K02] {
  type PTag = LSetTag03[K00, K01, K02]
  //  type Parent[_] = LSetPot04[K00, K01, K02, _]
  type Parent[_] = Nothing

  // TODO
  //  def addKey[KN: TypeTag](namer: Namer) = LSetPot04.create[K00, K01, K02, KN](namer.name(tag.plusKey[KN]), this)
  def plusKey[KN: TypeTag](namer: Namer) = ??? // TODO

}
