package loamstream.model.streams.atoms.sets

import loamstream.model.streams.sets.LSet01
import loamstream.model.tags.sets.LSetTag01

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
object LSetAtom01 {
  def create[K00: TypeTag](id: String) = LSetAtom01(id, LSetTag01.create[K00])
}

case class LSetAtom01[K00](id: String, tag: LSetTag01[K00]) extends LSet01[K00] {

}
