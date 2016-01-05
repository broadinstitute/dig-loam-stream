package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 10/21/2015.
  */
object LMapTag {
  type Map0[V] = LMapTag[Nothing, Nothing, V]
  type Map1[K0, V] = LMapTag[K0, LKeys.LKeys0, V]
  type Map2[K0, K1, V] = LMapTag[K0, LKeys.LKeys1[K1], V]
  type Map3[K0, K1, K2, V] = LMapTag[K0, LKeys.LKeys2[K1, K2], V]

  def forKeyTup0[V: TypeTag]: Map0[V] = LMapTag(LKeys.tup0, typeTag[V])

  def forKeyTup1[K0: TypeTag, V: TypeTag]: Map1[K0, V] = LMapTag(LKeys.tup1[K0], typeTag[V])

  def forKeyTup2[K0: TypeTag, K1: TypeTag, V: TypeTag]: Map2[K0, K1, V] = LMapTag(LKeys.tup2[K0, K1], typeTag[V])

  def forKeyTup3[K0: TypeTag, K1: TypeTag, K2: TypeTag, V: TypeTag]: Map3[K0, K1, K2, V] =
    LMapTag(LKeys.tup3[K0, K1, K2], typeTag[V])
}

case class LMapTag[HeadKey, KeysTail <: LKeys[_, _], V](keyTags: LKeys[HeadKey, KeysTail], vTag: TypeTag[V])
  extends LPileTag[HeadKey, KeysTail] {
  type UpTag[KeyNew] = LMapTag[KeyNew, LKeys[HeadKey, KeysTail], V]

  def toSet: LSetTag[HeadKey, KeysTail] = LSetTag(keyTags)

  override def plusKey[KeyNew: TypeTag]: UpTag[KeyNew] = LMapTag(typeTag[KeyNew] :: keyTags, vTag)
}
