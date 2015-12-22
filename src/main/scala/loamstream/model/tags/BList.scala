package loamstream.model.tags

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/22/2015.
  */
sealed trait BList[E, B[_], T <: BList[_, B, _]] {
  def ::[EN](head: B[EN]) = BLNode[EN, B, E, BList[E, B, T]](head, this)
}

object BNil extends BList[Nothing, Nothing, Nothing]

case class BLNode[E, B[_], EP, T <: BList[EP, B, _]](head: B[E], tail: T) extends BList[E, B, T]