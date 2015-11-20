package loamstream.model.streams.edges

import loamstream.model.streams.methods.LMethod
import loamstream.model.streams.piles.LPile
import loamstream.model.streams.sockets.LSocket

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
case class LEdge[P <: LPile, PT <: P#PTag, +M <: LMethod](pile: P, socket: LSocket[PT, M])
