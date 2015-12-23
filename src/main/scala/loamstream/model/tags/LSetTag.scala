package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/21/2015.
  */
case class LSetTag[HeadKey, KeysTail <: LKeys[_, _]](keyTags: LKeys[HeadKey, KeysTail])
  extends LPileTag[HeadKey, KeysTail] {
  type UpTag[KeyNew] = LSetTag[KeyNew, LKeys[HeadKey, KeysTail]]

  override def plusKey[KeyNew: TypeTag]: UpTag[KeyNew] = LSetTag(typeTag[KeyNew] :: keyTags)
}
