package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag[K, KT <: BList[Any, _, TypeTag, _]] {
  def keyTags: BList[Any, K, TypeTag, KT]

  type UpTag[KN] <: LPileTag[KN, BList[Any, K, TypeTag, KT]]

  def plusKey[KN: TypeTag]: UpTag[KN]
}
