package loamstream.model.streams.atoms.maps

import loamstream.model.streams.maps.LMap00
import loamstream.model.tags.maps.LMapTag00

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
object LMapAtom00 {
  def create[V: TypeTag](id: String) = LMapAtom00[V](id, LMapTag00.create[V])
}

case class LMapAtom00[V](id: String, tag: LMapTag00[V]) extends LMap00[V] {

}
