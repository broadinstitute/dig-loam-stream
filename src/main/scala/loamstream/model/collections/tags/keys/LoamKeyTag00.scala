package loamstream.model.collections.tags.keys

import loamstream.model.collections.tags.keys.LoamKeyTag.HasKeyTag
import loamstream.model.collections.tags.maps.LoamMapTag00
import loamstream.model.collections.tags.sets.LoamSetTag00

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
case object LoamKeyTag00 extends LoamKeyTag {

  trait HasKeyTag00 extends HasKeyTag {
    def key: LoamKeyTag00.type
  }

  override def plusKey[K00: TypeTag] = LoamKeyTag01(typeTag[K00])

  override def getLSet = LoamSetTag00

  override def getLMap[V: TypeTag] = LoamMapTag00(typeTag[V])
}
