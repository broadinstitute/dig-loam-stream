package loamstream.model.collections.tags.maps

import loamstream.model.collections.tags.piles.LPileTag
import loamstream.model.collections.tags.sets.LSetTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LMapTag[V] extends LPileTag {

  def vTag: TypeTag[V]

  def toSet: LSetTag

  override def plusKey[KN: TypeTag]: LMapTag[V]
}
