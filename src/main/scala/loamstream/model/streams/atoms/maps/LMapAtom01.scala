package loamstream.model.streams.atoms.maps

import loamstream.model.streams.maps.LMap01
import loamstream.model.tags.maps.LMapTag01

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
object LMapAtom01 {
  def create[K00: TypeTag, V: TypeTag](id: String) = LMapAtom01(id, LMapTag01.create[K00, V])
}

case class LMapAtom01[K00, V](id: String, tag: LMapTag01[K00, V]) extends LMap01[K00, V] {

}
