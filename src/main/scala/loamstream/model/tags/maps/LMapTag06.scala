package loamstream.model.tags.maps

import loamstream.model.tags.keys.LKeyTag06
import loamstream.model.tags.piles.LPileTag06
import loamstream.model.tags.sets.LSetTag06

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
object LMapTag06 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag, K04: TypeTag, K05: TypeTag, V: TypeTag] =
    LMapTag06(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03], typeTag[K04], typeTag[K05], typeTag[V])
}

case class LMapTag06[K00, K01, K02, K03, K04, K05, V]
(kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02], kTag03: TypeTag[K03], kTag04: TypeTag[K04],
 kTag05: TypeTag[K05], vTag: TypeTag[V])
  extends LMapTag[V] with LPileTag06[K00, K01, K02, K03, K04, K05] {
  type UpTag[_] = LMapTag07[K00, K01, K02, K03, K04, K05, _, V]

  override def plusKey[K06: TypeTag] = LMapTag07(kTag00, kTag01, kTag02, kTag03, kTag04, kTag05, typeTag[K06], vTag)

  override def key = LKeyTag06(kTag00, kTag01, kTag02, kTag03, kTag04, kTag05)

  override def toSet = LSetTag06(kTag00, kTag01, kTag02, kTag03, kTag04, kTag05)
}
