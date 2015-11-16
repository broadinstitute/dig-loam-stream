package loamstream.model.tags.sets

import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag
import scala.language.higherKinds

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LSetTag extends LPileTag {
  type UpTag[KN] <: LSetTag

  override def plusKey[KN: TypeTag]: UpTag[KN]
}
