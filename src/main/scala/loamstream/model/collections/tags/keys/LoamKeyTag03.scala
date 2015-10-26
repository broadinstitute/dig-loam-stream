package loamstream.model.collections.tags.keys

import loamstream.model.collections.tags.keys.LoamKeyTag00.HasKeyTag00
import loamstream.model.collections.tags.keys.LoamKeyTag01.HasKeyTag01
import loamstream.model.collections.tags.keys.LoamKeyTag02.HasKeyTag02
import loamstream.model.collections.tags.maps.{LoamMapTag03, LoamMapTag02}
import loamstream.model.collections.tags.sets.{LoamSetTag03, LoamSetTag02, LoamSetTag01}

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamKeyTag03 {

  trait HasKeyTag03[K00, K01, K02] extends HasKeyTag02[K00, K01] {
    def key: LoamKeyTag03[K00, K01, K02]
  }

  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag] = LoamKeyTag03(typeTag[K00], typeTag[K01], typeTag[K02])

}

case class LoamKeyTag03[K00, K01, K02](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02])
  extends LoamKeyTag {
  override def plusKey[K03: TypeTag] = LoamKeyTag04(kTag00, kTag01, kTag02, typeTag[K03])

  override def getLSet = LoamSetTag03(kTag00, kTag01, kTag02)

  override def getLMap[V: TypeTag] = LoamMapTag03(kTag00, kTag01, kTag02, typeTag[V])
}
