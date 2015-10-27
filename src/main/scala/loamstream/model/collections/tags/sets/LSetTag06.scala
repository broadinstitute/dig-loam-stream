package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.piles.{LPileTag06, LPileTag05}
import loamstream.model.collections.tags.keys.{LKeyTag06, LKeyTag05}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LSetTag06 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag, K04: TypeTag, K05: TypeTag] =
    LSetTag06(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03], typeTag[K04], typeTag[K05])
}

case class LSetTag06[K00, K01, K02, K03, K04, K05]
(kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02], kTag03: TypeTag[K03], kTag04: TypeTag[K04],
 kTag05: TypeTag[K05])
  extends LSetTag with LPileTag06[K00, K01, K02, K03, K04, K05] {
  override def key = LKeyTag06[K00, K01, K02, K03, K04, K05](kTag00, kTag01, kTag02, kTag03, kTag04, kTag05)

  override def plusKey[K06: TypeTag] =
    LSetTag07[K00, K01, K02, K03, K04, K05, K06](kTag00, kTag01,  kTag02,  kTag03, kTag04, kTag05, typeTag[K06])
}
