package loamstream.model.tags.maps

import loamstream.model.tags.keys.LKeyTag04
import loamstream.model.tags.piles.LPileTag04
import loamstream.model.tags.sets.LSetTag04

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
object LMapTag04 {
  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag, V: TypeTag] =
    LMapTag04(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03], typeTag[V])
}

case class LMapTag04[K00, K01, K02, K03, V](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02],
                                            kTag03: TypeTag[K03], vTag: TypeTag[V])
  extends LMapTag[V] with LPileTag04[K00, K01, K02, K03] {
  type UpTag[_] = LMapTag05[K00, K01, K02, K03, _, V]

  override def plusKey[K04: TypeTag] = LMapTag05(kTag00, kTag01, kTag02, kTag03, typeTag[K04], vTag)

  override def key = LKeyTag04(kTag00, kTag01, kTag02, kTag03)

  override def toSet = LSetTag04(kTag00, kTag01, kTag02, kTag03)
}
