package loamstream.model.tags

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/22/2015.
  */
sealed trait BList[EU, E, B[_], T <: BList[EU, _, B, _]] {
  def ::[EN](head: B[EN]) = BLNode[EU, EN, B, E, BList[EU, E, B, T]](head, this)
}

object BNil extends BList[Nothing, Nothing, Nothing, Nothing]

case class BLNode[EU, E, B[_], EP, T <: BList[EU, EP, B, _]](head: B[E], tail: T) extends BList[EU, E, B, T]