package loamstream.model.collections.tags.keys

import loamstream.model.collections.tags.keys.LKeyTag.HasKeyTag
import loamstream.model.collections.tags.maps.LMapTag04
import loamstream.model.collections.tags.sets.LSetTag04

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LKeyTag04 {

  trait HasKeyTag04[K00, K01, K02, K03] extends HasKeyTag {
    def key: LKeyTag04[K00, K01, K02, K03]
  }

  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag] =
    LKeyTag04(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03])

}

case class LKeyTag04[K00, K01, K02, K03](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02],
                                            kTag03: TypeTag[K03])
  extends LKeyTag {
  override def plusKey[K04: TypeTag] = LKeyTag05(kTag00, kTag01, kTag02, kTag03, typeTag[K04])

  override def getLSet = LSetTag04(kTag00, kTag01, kTag02, kTag03)

  override def getLMap[V: TypeTag] = LMapTag04(kTag00, kTag01, kTag02, kTag03, typeTag[V])
}
