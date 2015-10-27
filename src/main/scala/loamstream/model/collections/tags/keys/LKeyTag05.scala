package loamstream.model.collections.tags.keys

import loamstream.model.collections.tags.keys.LKeyTag.HasKeyTag
import loamstream.model.collections.tags.maps.LMapTag05
import loamstream.model.collections.tags.sets.LSetTag05

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LKeyTag05 {

  trait HasKeyTag05[K00, K01, K02, K03, K04] extends HasKeyTag {
    def key: LKeyTag05[K00, K01, K02, K03, K04]
  }

  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag, K04: TypeTag] =
    LKeyTag05(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03], typeTag[K04])

}

case class LKeyTag05[K00, K01, K02, K03, K04](kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02],
                                              kTag03: TypeTag[K03], kTag04: TypeTag[K04])
  extends LKeyTag {
  override def plusKey[K05: TypeTag] = LKeyTag06(kTag00, kTag01, kTag02, kTag03, kTag04, typeTag[K05])

  override def getLSet = LSetTag05(kTag00, kTag01, kTag02, kTag03, kTag04)

  override def getLMap[V: TypeTag] = LMapTag05(kTag00, kTag01, kTag02, kTag03, kTag04, typeTag[V])
}
