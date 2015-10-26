package loamstream.model.collections.tags.keys

import loamstream.model.collections.tags.maps.LoamMapTag
import loamstream.model.collections.tags.sets.LoamSetTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamKeyTag {

  trait HasKeyTag {
    def key : LoamKeyTag
  }

}

trait LoamKeyTag {

  def plusKey[KN: TypeTag]: LoamKeyTag

  def getLSet: LoamSetTag

  def getLMap[V: TypeTag]: LoamMapTag[V]

}
