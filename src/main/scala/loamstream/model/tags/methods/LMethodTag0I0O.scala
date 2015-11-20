package loamstream.model.tags.methods

import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/27/2015.
  */
case object LMethodTag0I0O extends LMethodTag {
  override def inputs: Seq[LPileTag] = Seq.empty

  override def outputs: Seq[LPileTag] = Seq.empty

  def plusKey[KN: TypeTag] = this
}
