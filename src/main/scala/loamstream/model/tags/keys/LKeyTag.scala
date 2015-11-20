package loamstream.model.tags.keys

import loamstream.model.tags.maps.LMapTag
import loamstream.model.tags.sets.LSetTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LKeyTag {

  trait HasKeyTag {
    def key : LKeyTag
  }

}

trait LKeyTag {

  def plusKey[KN: TypeTag]: LKeyTag

  def getLSet: LSetTag

  def getLMap[V: TypeTag]: LMapTag[V]

}
