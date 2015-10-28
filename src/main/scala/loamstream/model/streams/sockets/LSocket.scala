package loamstream.model.streams.sockets

import loamstream.model.streams.methods.LMethod
import loamstream.model.tags.methods.LMethodTag
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
trait LSocket[PT <: LPileTag, MT <: LMethodTag, M[_] <: LMethod[_]] {
  def method: M[MT]
  def pileTag: PT
}
