package loamstream.model.tags

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 12/22/2015.
  */
sealed trait LKeys[Key0, MoreKeys <: LKeys[_, _]] {
  def ::[KeyNew](head: TypeTag[KeyNew]) =
    LKeysNode[KeyNew, LKeys[Key0, MoreKeys]](head, this)
}

object LKeyNil extends LKeys[Nothing, Nothing]

case class LKeysNode[Key0, MoreKeys <: LKeys[_, _]](head: TypeTag[Key0], tail: MoreKeys)
  extends LKeys[Key0, MoreKeys]