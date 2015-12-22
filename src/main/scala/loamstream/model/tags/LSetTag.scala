package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/21/2015.
  */
case class LSetTag[K, KT <: BList[_, TypeTag, _]](keyTags: BList[K, TypeTag, KT]) extends LPileTag[K, KT] {
  type UpTag[KN] = LSetTag[KN, BList[K, TypeTag, KT]]

  override def plusKey[KN: TypeTag]: UpTag[KN] = LSetTag(typeTag[KN] :: keyTags)
}
