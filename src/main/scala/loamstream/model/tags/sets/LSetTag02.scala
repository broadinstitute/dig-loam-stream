package loamstream.model.tags.sets

import loamstream.model.tags.keys.LKeyTag02
import loamstream.model.tags.piles.LPileTag02

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
object LSetTag02 {
  def create[K00: TypeTag, K01: TypeTag] = LSetTag02(typeTag[K00], typeTag[K01])
}

case class LSetTag02[K00, K01](kTag00: TypeTag[K00], kTag01: TypeTag[K01])
  extends LSetTag with LPileTag02[K00, K01] {
  type UpTag[_] = LSetTag03[K00, K01, _]

  override def key = LKeyTag02[K00, K01](kTag00, kTag01)

  override def plusKey[K02: TypeTag] = LSetTag03[K00, K01, K02](kTag00, kTag01, typeTag[K02])
}
