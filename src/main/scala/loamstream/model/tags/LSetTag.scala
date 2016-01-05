package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/21/2015.
  */
object LSetTag {
  def forKeyTup1[K0: TypeTag] = LSetTag(LKeys.tup1[K0])

  def forKeyTup2[K0: TypeTag, K1: TypeTag] = LSetTag(LKeys.tup2[K0, K1])

  def forKeyTup3[K0: TypeTag, K1: TypeTag, K2: TypeTag] = LSetTag(LKeys.tup3[K0, K1, K2])
}

case class LSetTag[HeadKey, KeysTail <: LKeys[_, _]](keyTags: LKeys[HeadKey, KeysTail])
  extends LPileTag[HeadKey, KeysTail] {
  type UpTag[KeyNew] = LSetTag[KeyNew, LKeys[HeadKey, KeysTail]]

  override def plusKey[KeyNew: TypeTag]: UpTag[KeyNew] = LSetTag(typeTag[KeyNew] :: keyTags)
}
