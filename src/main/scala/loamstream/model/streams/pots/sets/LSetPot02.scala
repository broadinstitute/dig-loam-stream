package loamstream.model.streams.pots.sets

import loamstream.model.streams.sets.{LSet01, LSet02}
import loamstream.model.tags.sets.LSetTag02

import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
object LSetPot02 {
  def create[K00, K01: TypeTag](id: String, child: LSet01[K00]) =
    LSetPot02[K00, K01](id, child.tag.plusKey[K01], child)
}

case class LSetPot02[K00, K01](id: String, tag: LSetTag02[K00, K01], child: LSet01[K00])
  extends LSetPot[LSet01[K00]] with LSet02[K00, K01]
