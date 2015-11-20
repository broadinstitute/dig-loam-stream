package loamstream.model.streams.pots.maps

import loamstream.model.streams.maps.{LMap00, LMap01}
import loamstream.model.tags.maps.LMapTag01

import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
object LMapPot01 {
  def create[K00: TypeTag, V](id: String, child: LMap00[V]) = LMapPot01[K00, V](id, child.tag.plusKey[K00], child)
}

case class LMapPot01[K00, V](id: String, tag: LMapTag01[K00, V], child: LMap00[V])
  extends LMapPot[V, LMap00[V]] with LMap01[K00, V]
