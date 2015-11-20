package loamstream.model.streams.piles

import loamstream.model.streams.pots.piles.LPilePot
import loamstream.model.tags.piles.LPileTag00

import scala.language.higherKinds


/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LPile00 extends LPile {
  type PTag <: LPileTag00
  type Parent[KN] <: LPilePot[LPile]
}
