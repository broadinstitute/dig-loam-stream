package loamstream.model.streams.pots.maps

import loamstream.model.streams.maps.{LMap02, LMap03}
import loamstream.model.tags.maps.LMapTag03

import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
object LMapPot03 {
  def create[K00, K01, K02: TypeTag, V](id: String, child: LMap02[K00, K01, V]) =
    LMapPot03[K00, K01, K02, V](id, child.tag.plusKey[K02], child)
}

case class LMapPot03[K00, K01, K02, V](id: String, tag: LMapTag03[K00, K01, K02, V], child: LMap02[K00, K01, V])
  extends LMapPot[V, LMap02[K00, K01, V]] with LMap03[K00, K01, K02, V]
