package loamstream.model.tags.sets

import loamstream.model.tags.keys.LKeyTag03
import loamstream.model.tags.piles.LPileTag03

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
object LSetTag03 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag] = LSetTag03(typeTag[K00], typeTag[K01], typeTag[K02])
}

case class LSetTag03[K00, K01, K02](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02])
  extends LSetTag with LPileTag03[K00, K01, K02] {
  type UpTag[KN] = LSetTag04[K00, K01, K02, KN]

  override def key = LKeyTag03[K00, K01, K02](kTag00, kTag01, kTag02)

  override def plusKey[K03: TypeTag] = LSetTag04[K00, K01, K02, K03](kTag00, kTag01, kTag02, typeTag[K03])
}
