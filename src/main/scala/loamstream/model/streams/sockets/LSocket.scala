package loamstream.model.streams.sockets

import loamstream.model.streams.methods.LMethod
import loamstream.model.tags.piles.LPileTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LSocket[PT <: LPileTag, M <: LMethod] {
  def method: M

  def pileTag: PT
}
