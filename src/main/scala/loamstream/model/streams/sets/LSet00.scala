package loamstream.model.streams.sets

import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.piles.LPile00
import loamstream.model.streams.pots.sets.LSetPot01
import loamstream.model.tags.sets.LSetTag00

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSet00 extends LSet with LPile00 {
  type PTag = LSetTag00.type
  type Parent[KN] = LSetPot01[KN]

  def plusKey[KN: TypeTag](namer: Namer) = LSetPot01.create[KN](namer.name(tag.plusKey[KN]), this)
}
