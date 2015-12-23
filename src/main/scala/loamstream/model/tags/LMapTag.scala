package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/21/2015.
  */
case class LMapTag[K, KT <: BList[Any, _, TypeTag, _], V](keyTags: BList[Any, K, TypeTag, KT], vTag: TypeTag[V])
  extends LPileTag[K, KT] {
  type UpTag[KN] = LMapTag[KN, BList[Any, K, TypeTag, KT], V]

  def toSet: LSetTag[K, KT] = LSetTag(keyTags)

  override def plusKey[KN: TypeTag]: UpTag[KN] = LMapTag(typeTag[KN] :: keyTags, vTag)
}
