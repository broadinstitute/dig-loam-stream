package loamstream.model.tags.maps

import loamstream.model.tags.keys.LKeyTag02
import loamstream.model.tags.piles.LPileTag02
import loamstream.model.tags.sets.LSetTag02

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
object LMapTag02 {
  def create[K00: TypeTag, K01: TypeTag, V: TypeTag] = LMapTag02(typeTag[K00], typeTag[K01], typeTag[V])
}

case class LMapTag02[K00, K01, V](kTag00: TypeTag[K00], kTag01: TypeTag[K01], vTag: TypeTag[V])
  extends LMapTag[V] with LPileTag02[K00, K01] {
  type UpTag[_] = LMapTag03[K00, K01, _, V]

  override def plusKey[K02: TypeTag] = LMapTag03(kTag00, kTag01, typeTag[K02], vTag)

  override def key = LKeyTag02(kTag00, kTag01)

  override def toSet = LSetTag02(kTag00, kTag01)
}
