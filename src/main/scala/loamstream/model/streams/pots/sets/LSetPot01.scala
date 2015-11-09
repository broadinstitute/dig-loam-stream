package loamstream.model.streams.pots.sets

import loamstream.model.streams.sets.{LSet00, LSet01}
import loamstream.model.tags.sets.LSetTag01

import scala.reflect.runtime.universe.TypeTag


/**
  * LoamStream
  * Created by oliverr on 11/9/2015.
  */
object LSetPot01 {
  def create[K00: TypeTag](id: String, child: LSet00) = LSetPot01(id, child.tag.plusKey[K00], child)
}

case class LSetPot01[K00](id: String, tag: LSetTag01[K00], child: LSet00) extends LSetPot[LSet00] with LSet01[K00]
