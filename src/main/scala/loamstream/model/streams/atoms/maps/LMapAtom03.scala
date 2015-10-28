package loamstream.model.streams.atoms.maps

import loamstream.model.streams.maps.LMap03
import loamstream.model.tags.maps.LMapTag03

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
object LMapAtom03 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, V: TypeTag](id: String) =
    LMapAtom03(id, LMapTag03.create[K00, K01, K02, V])
}

case class LMapAtom03[K00, K01, K02, V](id: String, tag: LMapTag03[K00, K01, K02, V])
  extends LMap03[K00, K01, K02, V] {

}
