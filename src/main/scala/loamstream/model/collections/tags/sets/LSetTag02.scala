package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.piles.{LPileTag02, LPileTag01}
import loamstream.model.collections.tags.keys.{LKeyTag02, LKeyTag01}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LSetTag02 {
  def create[K00: TypeTag, K01:TypeTag] = LSetTag02(typeTag[K00], typeTag[K01])
}

case class LSetTag02[K00, K01](kTag00: TypeTag[K00], kTag01: TypeTag[K01])
  extends LSetTag with LPileTag02[K00, K01] {
  override def key = LKeyTag02[K00, K01](kTag00, kTag01)

  override def plusKey[K02: TypeTag] = LSetTag03[K00, K01, K02](kTag00, kTag01, typeTag[K02])
}
