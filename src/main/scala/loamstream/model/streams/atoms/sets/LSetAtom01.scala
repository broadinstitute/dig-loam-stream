package loamstream.model.streams.atoms.sets

import loamstream.model.streams.sets.LSet01
import loamstream.model.tags.sets.LSetTag01

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
case class LSetAtom01[K00: TypeTag](id: String) extends LSet01[K00] {

  val tag = LSetTag01.create[K00]

}
