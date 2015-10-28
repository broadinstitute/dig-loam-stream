package loamstream.model.streams.atoms.sets

import loamstream.model.streams.sets.LSet02
import loamstream.model.tags.sets.LSetTag02

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
object LSetAtom02 {
  def create[K00: TypeTag, K01: TypeTag](id: String) = LSetAtom02(id, LSetTag02.create[K00, K01])
}

case class LSetAtom02[K00, K01](id: String, tag: LSetTag02[K00, K01]) extends LSet02[K00, K01] {

}
