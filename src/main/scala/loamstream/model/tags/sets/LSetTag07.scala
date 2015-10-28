package loamstream.model.tags.sets

import loamstream.model.tags.piles.LPileTag07
import loamstream.model.tags.keys.LKeyTag07

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LSetTag07 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag, K04: TypeTag, K05: TypeTag, K06: TypeTag] =
    LSetTag07(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03], typeTag[K04], typeTag[K05], typeTag[K05])
}

case class LSetTag07[K00, K01, K02, K03, K04, K05, K06]
(kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02], kTag03: TypeTag[K03], kTag04: TypeTag[K04],
 kTag05: TypeTag[K05], kTag06: TypeTag[K06])
  extends LSetTag with LPileTag07[K00, K01, K02, K03, K04, K05, K06] {
  override def key = LKeyTag07[K00, K01, K02, K03, K04, K05, K06](kTag00, kTag01, kTag02, kTag03, kTag04, kTag05,
  kTag06)

  //  override def plusKey[K07: TypeTag] =
  //    LSetTag08[K00, K01, K02, K03, K04, K05, K06](kTag00, kTag01,  kTag02,  kTag03, kTag04, kTag05, kTag05,
  //    typeTag[K06])
  override def plusKey[K07: TypeTag]: LSetTag = ??? //TODO
}
