package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.piles.LPileTag04
import loamstream.model.collections.tags.keys.LKeyTag04

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LSetTag04 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag] =
    LSetTag04(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03])
}

case class LSetTag04[K00, K01, K02, K03](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02],
                                            kTag03: TypeTag[K03])
  extends LSetTag with LPileTag04[K00, K01, K02, K03] {
  override def key = LKeyTag04[K00, K01, K02, K03](kTag00, kTag01, kTag02, kTag03)

  override def plusKey[K04: TypeTag] =
    LSetTag05[K00, K01, K02, K03, K04](kTag00, kTag01,  kTag02,  kTag03, typeTag[K04])
}
