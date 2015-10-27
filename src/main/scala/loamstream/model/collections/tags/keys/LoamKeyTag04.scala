package loamstream.model.collections.tags.keys

import loamstream.model.collections.tags.keys.LoamKeyTag03.HasKeyTag03
import loamstream.model.collections.tags.maps.LoamMapTag04
import loamstream.model.collections.tags.sets.LoamSetTag04

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LoamKeyTag04 {

  trait HasKeyTag04[K00, K01, K02, K03] extends HasKeyTag03[K00, K01, K02] {
    def key: LoamKeyTag04[K00, K01, K02, K03]
  }

  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag] =
    LoamKeyTag04(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03])

}

case class LoamKeyTag04[K00, K01, K02, K03](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02],
                                            kTag03: TypeTag[K03])
  extends LoamKeyTag {
  override def plusKey[K04: TypeTag] = LoamKeyTag05(kTag00, kTag01, kTag02, kTag03, typeTag[K04])

  override def getLSet = LoamSetTag04(kTag00, kTag01, kTag02, kTag03)

  override def getLMap[V: TypeTag] = LoamMapTag04(kTag00, kTag01, kTag02, kTag03, typeTag[V])
}
