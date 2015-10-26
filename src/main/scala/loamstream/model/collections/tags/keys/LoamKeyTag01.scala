package loamstream.model.collections.tags.keys

import loamstream.model.collections.tags.keys.LoamKeyTag00.HasKeyTag00
import loamstream.model.collections.tags.maps.LoamMapTag01
import loamstream.model.collections.tags.sets.LoamSetTag01

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamKeyTag01 {

  trait HasKeyTag01[K00] extends HasKeyTag00 {
    def key: LoamKeyTag01[K00]
  }

  def create[K00: TypeTag] = LoamKeyTag01(typeTag[K00])

}

case class LoamKeyTag01[K00](kTag00: TypeTag[K00]) extends LoamKeyTag {
  override def plusKey[K01: TypeTag] = LoamKeyTag02[K00, K01](kTag00, typeTag[K01])

  override def getLSet = LoamSetTag01(kTag00)

  override def getLMap[V: TypeTag] = LoamMapTag01(kTag00, typeTag[V])
}
