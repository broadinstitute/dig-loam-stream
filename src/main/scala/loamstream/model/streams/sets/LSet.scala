package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile
import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.pots.sets.LSetPot
import loamstream.model.tags.sets.LSetTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet extends LPile {
  type PTag <: LSetTag
  type Parent[_] <: LSetPot[LSet]

  def plusKey[KN: TypeTag](namer: Namer): Parent[KN]

}
