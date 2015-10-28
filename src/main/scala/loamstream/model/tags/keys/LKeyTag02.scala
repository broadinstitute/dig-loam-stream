package loamstream.model.tags.keys

import loamstream.model.tags.keys.LKeyTag.HasKeyTag
import loamstream.model.tags.keys.LKeyTag01.HasKeyTag01
import loamstream.model.tags.maps.LMapTag02
import loamstream.model.tags.sets.LSetTag02

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LKeyTag02 {

  trait HasKeyTag02[K00, K01] extends HasKeyTag {
    def key: LKeyTag02[K00, K01]
  }

  def create[K00: TypeTag, K01: TypeTag] = LKeyTag02(typeTag[K00], typeTag[K01])

}

case class LKeyTag02[K00, K01](kTag00: TypeTag[K00], kTag01: TypeTag[K01]) extends LKeyTag {
  override def plusKey[K02: TypeTag] = LKeyTag03(kTag00, kTag01, typeTag[K02])

  override def getLSet = LSetTag02(kTag00, kTag01)

  override def getLMap[V: TypeTag] = LMapTag02(kTag00, kTag01, typeTag[V])
}
