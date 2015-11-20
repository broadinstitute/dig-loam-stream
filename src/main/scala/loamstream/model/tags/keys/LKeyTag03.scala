package loamstream.model.tags.keys

import loamstream.model.tags.keys.LKeyTag.HasKeyTag
import loamstream.model.tags.maps.LMapTag03
import loamstream.model.tags.sets.LSetTag03

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LKeyTag03 {

  trait HasKeyTag03[K00, K01, K02] extends HasKeyTag {
    def key: LKeyTag03[K00, K01, K02]
  }

  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag] = LKeyTag03(typeTag[K00], typeTag[K01], typeTag[K02])

}

case class LKeyTag03[K00, K01, K02](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02])
  extends LKeyTag {
  override def plusKey[K03: TypeTag] = LKeyTag04(kTag00, kTag01, kTag02, typeTag[K03])

  override def getLSet = LSetTag03(kTag00, kTag01, kTag02)

  override def getLMap[V: TypeTag] = LMapTag03(kTag00, kTag01, kTag02, typeTag[V])
}
