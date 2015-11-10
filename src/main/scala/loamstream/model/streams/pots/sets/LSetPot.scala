package loamstream.model.streams.pots.sets

import loamstream.model.streams.pots.piles.LPilePot
import loamstream.model.streams.sets.LSet

/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
trait LSetPot[+C <: LSet] extends LSet with LPilePot[C] {
  def child: C
}
