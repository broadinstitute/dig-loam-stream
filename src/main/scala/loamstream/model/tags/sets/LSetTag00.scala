package loamstream.model.tags.sets

import loamstream.model.tags.piles.LPileTag00
import loamstream.model.tags.keys.LKeyTag00

import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.language.higherKinds

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
case object LSetTag00 extends LSetTag with LPileTag00 {
  type UpTag[KN] = LSetTag01[KN]

  override def key = LKeyTag00

  override def plusKey[K00: TypeTag] = LSetTag01[K00](typeTag[K00])
}
