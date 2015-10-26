package loamstream.model.collections.tags.keys

import loamstream.model.collections.tags.keys.LoamKeyTag00.HasKeyTag00
import loamstream.model.collections.tags.keys.LoamKeyTag01.HasKeyTag01
import loamstream.model.collections.tags.maps.LoamMapTag02
import loamstream.model.collections.tags.sets.{LoamSetTag02, LoamSetTag01}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamKeyTag02 {

  trait HasKeyTag02[K00, K01] extends HasKeyTag01[K00] {
    def key: LoamKeyTag02[K00, K01]
  }

  def create[K00: TypeTag, K01: TypeTag] = LoamKeyTag02(typeTag[K00], typeTag[K01])

}

case class LoamKeyTag02[K00, K01](kTag00: TypeTag[K00], kTag01: TypeTag[K01]) extends LoamKeyTag {
  override def plusKey[K02: TypeTag] = LoamKeyTag03(kTag00, kTag01, typeTag[K02])

  override def getLSet = LoamSetTag02(kTag00, kTag01)

  override def getLMap[V: TypeTag] = LoamMapTag02(kTag00, kTag01, typeTag[V])
}
