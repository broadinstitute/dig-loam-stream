package loamstream.model.streams.pots.piles

import loamstream.model.streams.piles.LPile

/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
trait LPilePot[+C <: LPile] extends LPile {
  def child: C
}
