package loamstream.model.streams.atoms.sets

import loamstream.model.streams.sets.LSet00
import loamstream.model.tags.sets.LSetTag00

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
case class LSetAtom00(id: String) extends LSet00 {
  override def tag = LSetTag00
}
