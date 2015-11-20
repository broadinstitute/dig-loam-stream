package loamstream.model.tags.maps

import loamstream.model.tags.piles.LPileTag
import loamstream.model.tags.sets.LSetTag

import scala.reflect.runtime.universe.TypeTag
import scala.language.higherKinds

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LMapTag[V] extends LPileTag {
  type UpTag[KN] <: LMapTag[V]

  def vTag: TypeTag[V]

  def toSet: LSetTag

  override def plusKey[KN: TypeTag]: UpTag[KN]
}
