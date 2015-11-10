package loamstream.model.tags.maps

import loamstream.model.tags.keys.LKeyTag05
import loamstream.model.tags.piles.LPileTag05
import loamstream.model.tags.sets.LSetTag05

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
object LMapTag05 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag, K04: TypeTag, V: TypeTag] =
    LMapTag05(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03], typeTag[K04], typeTag[V])
}

case class LMapTag05[K00, K01, K02, K03, K04, V](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02],
                                                 kTag03: TypeTag[K03], kTag04: TypeTag[K04], vTag: TypeTag[V])
  extends LMapTag[V] with LPileTag05[K00, K01, K02, K03, K04] {
  type UpTag[_] = LMapTag06[K00, K01, K02, K03, K04, _, V]

  override def plusKey[K05: TypeTag] = LMapTag06(kTag00, kTag01, kTag02, kTag03, kTag04, typeTag[K05], vTag)

  override def key = LKeyTag05(kTag00, kTag01, kTag02, kTag03, kTag04)

  override def toSet = LSetTag05(kTag00, kTag01, kTag02, kTag03, kTag04)
}
