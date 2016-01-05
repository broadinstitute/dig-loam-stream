package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 12/22/2015.
  */
object LKeys {
  type LKeys0 = LKeys[Nothing, Nothing]
  type LKeys1[K0] = LKeys[K0, LKeys[Nothing, Nothing]]
  type LKeys2[K0, K1] = LKeys[K0, LKeys[K1, LKeys[Nothing, Nothing]]]
  type LKeys3[K0, K1, K2] = LKeys[K0, LKeys[K1, LKeys[K2, LKeys[Nothing, Nothing]]]]

  def tup0: LKeys0 = LKeyNil

  def tup1[K0: TypeTag]: LKeys1[K0] = typeTag[K0] :: LKeyNil

  def tup2[K0: TypeTag, K1: TypeTag]: LKeys2[K0, K1] = typeTag[K0] :: typeTag[K1] :: LKeyNil

  def tup3[K0: TypeTag, K1: TypeTag, K2: TypeTag]: LKeys3[K0, K1, K2] =
    typeTag[K0] :: typeTag[K1] :: typeTag[K2] :: LKeyNil
}

sealed trait LKeys[+Key0, +MoreKeys <: LKeys[_, _]] {
  def ::[KeyNew](head: TypeTag[KeyNew]) =
    LKeysNode[KeyNew, LKeys[Key0, MoreKeys]](head, this)

  def prepend[KN: TypeTag] = typeTag[KN] :: this
}

object LKeyNil extends LKeys[Nothing, Nothing]

case class LKeysNode[Key0, +MoreKeys <: LKeys[_, _]](head: TypeTag[Key0], tail: MoreKeys)
  extends LKeys[Key0, MoreKeys]