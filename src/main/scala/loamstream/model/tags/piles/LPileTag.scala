package loamstream.model.tags.piles

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/26/2015.
  */
trait LPileTag {
  type UpTag[KN] <: LPileTag

  def plusKey[KN: TypeTag]: UpTag[KN]
}
