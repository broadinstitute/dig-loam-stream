package loamstream.model.tags.sets

import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LSetTag extends LPileTag {
  override def plusKey[TC: TypeTag]: LSetTag
}
