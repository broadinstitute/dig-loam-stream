package loamstream.model.tags.keys

import loamstream.model.tags.keys.LKeyTag.HasKeyTag
import loamstream.model.tags.maps.LMapTag07
import loamstream.model.tags.sets.LSetTag07

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
object LKeyTag07 {

  trait HasKeyTag07[K00, K01, K02, K03, K04, K05, K06] extends HasKeyTag {
    def key: LKeyTag07[K00, K01, K02, K03, K04, K05, K06]
  }

  def create[K00: TypeTag, K01: TypeTag, K02: TypeTag, K03: TypeTag, K04: TypeTag, K05: TypeTag, K06: TypeTag] =
    LKeyTag07(typeTag[K00], typeTag[K01], typeTag[K02], typeTag[K03], typeTag[K04], typeTag[K05], typeTag[K06])

}

case class LKeyTag07[K00, K01, K02, K03, K04, K05, K06]
(kTag00: TypeTag[K00], kTag01: TypeTag[K01], kTag02: TypeTag[K02], kTag03: TypeTag[K03], kTag04: TypeTag[K04],
 kTag05: TypeTag[K05], kTag06: TypeTag[K06])
  extends LKeyTag {
  //  override def plusKey[K07: TypeTag] = LKeyTag08(kTag00, kTag01, kTag02, kTag03, kTag04, kTag05, kTag06, typeTag[K06])
  override def plusKey[K07: TypeTag]: LKeyTag = ??? // TODO

  override def getLSet = LSetTag07(kTag00, kTag01, kTag02, kTag03, kTag04, kTag05, kTag06)

  override def getLMap[V: TypeTag] = LMapTag07(kTag00, kTag01, kTag02, kTag03, kTag04, kTag05, kTag06, typeTag[V])
}
