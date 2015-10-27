package loamstream.model.collections.tags.piles

import loamstream.model.collections.tags.keys.LKeyTag.HasKeyTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
trait LPileTag extends HasKeyTag {
  def plusKey[TC: TypeTag]: LPileTag
}
