package loamstream.model.streams.atoms.sets

import loamstream.model.streams.sets.LSet03
import loamstream.model.tags.sets.LSetTag03

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
object LSetAtom03 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag](id: String) = LSetAtom03(id, LSetTag03.create[K00, K01, K02])
}

case class LSetAtom03[K00, K01, K02](id: String, tag: LSetTag03[K00, K01, K02]) extends LSet03[K00, K01, K02] {

}
