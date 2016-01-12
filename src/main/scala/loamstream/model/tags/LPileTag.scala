package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag[HeadKey, KeysTail <: LKeys[_, _]] {
  type K0 = HeadKey

  def keyTags: LKeys[HeadKey, KeysTail]

  type UpTag[KeyNew] <: LPileTag[KeyNew, LKeys[HeadKey, KeysTail]]

  def plusKey[KeyNew: TypeTag]: UpTag[KeyNew]
}
