package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/21/2015.
  */
case class LMapTag[HeadKey, KeysTail <: LKeys[_, _], V](keyTags: LKeys[HeadKey, KeysTail], vTag: TypeTag[V])
  extends LPileTag[HeadKey, KeysTail] {
  type UpTag[KeyNew] = LMapTag[KeyNew, LKeys[HeadKey, KeysTail], V]

  def toSet: LSetTag[HeadKey, KeysTail] = LSetTag(keyTags)

  override def plusKey[KeyNew: TypeTag]: UpTag[KeyNew] = LMapTag(typeTag[KeyNew] :: keyTags, vTag)
}
