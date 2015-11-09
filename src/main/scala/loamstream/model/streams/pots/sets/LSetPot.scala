package loamstream.model.streams.pots.sets

import loamstream.model.streams.sets.LSet

/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
trait LSetPot[C] extends LSet {
  def child: C
}
