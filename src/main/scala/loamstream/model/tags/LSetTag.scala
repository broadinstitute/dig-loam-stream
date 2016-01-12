package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/21/2015.
  */
object LSetTag {
  type Set0 = LSetTag[Nothing, Nothing]
  type Set1[K0] = LSetTag[K0, LKeys.LKeys0]
  type Set2[K0, K1] = LSetTag[K0, LKeys.LKeys1[K1]]
  type Set3[K0, K1, K2] = LSetTag[K0, LKeys.LKeys2[K1, K2]]

  def forKeyTup1[K0: TypeTag]: Set1[K0] = LSetTag(LKeys.tup1[K0])

  def forKeyTup2[K0: TypeTag, K1: TypeTag]: Set2[K0, K1] = LSetTag(LKeys.tup2[K0, K1])

  def forKeyTup3[K0: TypeTag, K1: TypeTag, K2: TypeTag]: Set3[K0, K1, K2] = LSetTag(LKeys.tup3[K0, K1, K2])
}

case class LSetTag[HeadKey, KeysTail <: LKeys[_, _]](keyTags: LKeys[HeadKey, KeysTail])
  extends LPileTag[HeadKey, KeysTail] {
  type UpTag[KeyNew] = LSetTag[KeyNew, LKeys[HeadKey, KeysTail]]

  override def plusKey[KeyNew: TypeTag]: UpTag[KeyNew] = LSetTag(typeTag[KeyNew] :: keyTags)
}
