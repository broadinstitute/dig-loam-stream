package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LMapTag[K, KT <: BList[_, TypeTag, _], V] extends LPileTag[K, KT] {
  type UpTag[KN] <: LMapTag[KN, BList[K, TypeTag, KT], V]

  def vTag: TypeTag[V]

  def toSet: LSetTag[K, KT]

  override def plusKey[KN: TypeTag]: UpTag[KN]
}
