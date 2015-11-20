package loamstream.model.streams.pots.sets

import loamstream.model.streams.sets.{LSet02, LSet03}
import loamstream.model.tags.sets.LSetTag03

import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
object LSetPot03 {
  def create[K00, K01, K02: TypeTag](id: String, child: LSet02[K00, K01]) =
    LSetPot03[K00, K01, K02](id, child.tag.plusKey[K02], child)
}


case class LSetPot03[K00, K01, K02](id: String, tag: LSetTag03[K00, K01, K02], child: LSet02[K00, K01])
  extends LSetPot[LSet02[K00, K01]] with LSet03[K00, K01, K02]
