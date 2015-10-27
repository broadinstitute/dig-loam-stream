package loamstream.model.collections.tags.keys

import loamstream.model.collections.tags.keys.LKeyTag.HasKeyTag
import loamstream.model.collections.tags.maps.LMapTag01
import loamstream.model.collections.tags.sets.LSetTag01

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LKeyTag01 {

  trait HasKeyTag01[K00] extends HasKeyTag {
    def key: LKeyTag01[K00]
  }

  def create[K00: TypeTag] = LKeyTag01(typeTag[K00])

}

case class LKeyTag01[K00](kTag00: TypeTag[K00]) extends LKeyTag {
  override def plusKey[K01: TypeTag] = LKeyTag02[K00, K01](kTag00, typeTag[K01])

  override def getLSet = LSetTag01(kTag00)

  override def getLMap[V: TypeTag] = LMapTag01(kTag00, typeTag[V])
}
