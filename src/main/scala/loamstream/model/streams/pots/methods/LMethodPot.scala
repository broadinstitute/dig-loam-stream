package loamstream.model.streams.pots.methods

import loamstream.model.streams.methods.LMethod

/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
trait LMethodPot[+C <: LMethod] extends LMethod {
  def child: C
}
