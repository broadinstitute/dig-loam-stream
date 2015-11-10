package loamstream.model.tags.sets

import loamstream.model.tags.keys.LKeyTag05
import loamstream.model.tags.piles.LPileTag05

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
object LSetTag05 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag, K04: TypeTag] =
    LSetTag05(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03], typeTag[K04])
}

case class LSetTag05[K00, K01, K02, K03, K04]
(kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02], kTag03: TypeTag[K03], kTag04: TypeTag[K04])
  extends LSetTag with LPileTag05[K00, K01, K02, K03, K04] {
  type UpTag[_] = LSetTag06[K00, K01, K02, K03, K04, _]

  override def key = LKeyTag05[K00, K01, K02, K03, K04](kTag00, kTag01, kTag02, kTag03, kTag04)

  override def plusKey[K05: TypeTag] =
    LSetTag06[K00, K01, K02, K03, K04, K05](kTag00, kTag01, kTag02, kTag03, kTag04, typeTag[K05])
}
