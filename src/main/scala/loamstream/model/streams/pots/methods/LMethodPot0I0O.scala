package loamstream.model.streams.pots.methods

import loamstream.model.streams.methods.LMethod0I0O

/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
case class LMethodPot0I0O[C <: LMethod0I0O](id: String, child: C) extends LMethodPot[C] with LMethod0I0O {

}
