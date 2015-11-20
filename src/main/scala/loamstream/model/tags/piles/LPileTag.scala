package loamstream.model.tags.piles

import loamstream.model.tags.keys.LKeyTag.HasKeyTag

import scala.reflect.runtime.universe.TypeTag
import scala.language.higherKinds
/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag extends HasKeyTag {
  type UpTag[KN] <: LPileTag

  def plusKey[KN: TypeTag]: UpTag[KN]
}
