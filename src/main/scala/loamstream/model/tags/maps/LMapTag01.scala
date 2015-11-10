package loamstream.model.tags.maps

import loamstream.model.tags.keys.LKeyTag01
import loamstream.model.tags.piles.LPileTag01
import loamstream.model.tags.sets.LSetTag01

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
object LMapTag01 {
  def create[K00: TypeTag, V: TypeTag] = LMapTag01(typeTag[K00], typeTag[V])
}

case class LMapTag01[K00, V](kTag00: TypeTag[K00], vTag: TypeTag[V]) extends LMapTag[V] with LPileTag01[K00] {
  type UpTag[_] = LMapTag02[K00, _, V]

  override def plusKey[K01: TypeTag] = LMapTag02(kTag00, typeTag[K01], vTag)

  override def key = LKeyTag01(kTag00)

  override def toSet = LSetTag01(kTag00)
}
