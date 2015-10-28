package loamstream.model.tags.keys

import loamstream.model.tags.keys.LKeyTag.HasKeyTag
import loamstream.model.tags.maps.LMapTag00
import loamstream.model.tags.sets.LSetTag00

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
case object LKeyTag00 extends LKeyTag {

  trait HasKeyTag00 extends HasKeyTag {
    def key: LKeyTag00.type
  }

  override def plusKey[K00: TypeTag] = LKeyTag01(typeTag[K00])

  override def getLSet = LSetTag00

  override def getLMap[V: TypeTag] = LMapTag00(typeTag[V])
}
