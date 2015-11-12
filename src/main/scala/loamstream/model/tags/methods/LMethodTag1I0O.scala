package loamstream.model.tags.methods

import loamstream.model.tags.methods.LMethodTag.Has1I
import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/27/2015.
  */
case class LMethodTag1I0O[I0 <: LPileTag](input0: I0) extends LMethodTag with LMethodTag.Has1I[I0] {
  override def inputs: Seq[LPileTag] = Seq(input0)

  override def outputs: Seq[LPileTag] = Seq.empty

  override def plusKeyI0[KN: TypeTag] = LMethodTag1I0O[I0#UpTag[KN]](input0.plusKey[KN])
}
