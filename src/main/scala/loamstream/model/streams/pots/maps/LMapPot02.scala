package loamstream.model.streams.pots.maps

import loamstream.model.streams.maps.{LMap01, LMap02}
import loamstream.model.tags.maps.LMapTag02

import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
object LMapPot02 {
  def create[K00, K01: TypeTag, V](id: String, child: LMap01[K00, V]) =
    LMapPot02[K00, K01, V](id, child.tag.plusKey[K01], child)
}

case class LMapPot02[K00, K01, V](id: String, tag: LMapTag02[K00, K01, V], child: LMap01[K00, V])
  extends LMapPot[V, LMap01[K00, V]] with LMap02[K00, K01, V]
