package loamstream.model.tags.sets

import loamstream.model.tags.piles.LPileTag01
import loamstream.model.tags.keys.LKeyTag01

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LSetTag01 {
  def create[K00: TypeTag] = LSetTag01(typeTag[K00])
}

case class LSetTag01[K00](kTag00: TypeTag[K00]) extends LSetTag with LPileTag01[K00] {
  override def key = LKeyTag01[K00](kTag00)

  override def plusKey[K01: TypeTag] = LSetTag02[K00, K01](kTag00, typeTag[K01])
}
