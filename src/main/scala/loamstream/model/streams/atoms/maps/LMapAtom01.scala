package loamstream.model.streams.atoms.maps

import loamstream.model.streams.maps.LMap01
import loamstream.model.tags.maps.LMapTag01

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
case class LMapAtom01[K00:TypeTag, V: TypeTag](id: String) extends LMap01[K00, V] {

  val tag = LMapTag01.create[K00, V]

}
