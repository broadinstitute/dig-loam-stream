package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.piles.LPileTag00
import loamstream.model.collections.tags.keys.LKeyTag00

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
case object LSetTag00 extends LSetTag with LPileTag00 {
  override def key = LKeyTag00

  override def plusKey[K00: TypeTag] = LSetTag01[K00](typeTag[K00])
}
