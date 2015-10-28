package loamstream.model.streams.atoms.maps

import loamstream.model.streams.maps.LMap00
import loamstream.model.tags.maps.LMapTag00

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
case class LMapAtom00[V: TypeTag](id: String) extends LMap00[V] {

  val tag = LMapTag00.create[V]

}
