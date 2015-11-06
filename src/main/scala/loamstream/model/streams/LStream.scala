package loamstream.model.streams

import loamstream.model.streams.edges.LEdge
import loamstream.model.streams.methods.LMethod
import loamstream.model.streams.piles.LPile

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
trait LStream {
  def piles: Iterable[LPile]

  def methods: Iterable[LMethod]

  def edges: Iterable[LEdge[_, _, _]]

}
