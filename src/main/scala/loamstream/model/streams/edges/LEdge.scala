package loamstream.model.streams.edges

import loamstream.model.streams.methods.LMethod
import loamstream.model.streams.piles.LPile
import loamstream.model.streams.sockets.LSocket
import loamstream.model.tags.methods.LMethodTag
import loamstream.model.tags.piles.LPileTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
trait LEdge[PT <: LPileTag, MT <: LMethodTag, P[_] <: LPile[_], M[_] <: LMethod[_], S[_, _, _] <: LSocket[_, _, _]] {
  def pile: P[PT]

  def socket: S[PT, MT, M[MT]]
}
