package loamstream.model.streams.atoms.maps

import loamstream.model.streams.maps.LMap02
import loamstream.model.tags.maps.LMapTag02

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
object LMapAtom02 {
  def create[K00: TypeTag, K01: TypeTag, V: TypeTag](id: String) = LMapAtom02(id, LMapTag02.create[K00, K01, V])
}

case class LMapAtom02[K00, K01, V](id: String, tag: LMapTag02[K00, K01, V]) extends LMap02[K00, K01, V] {

}
