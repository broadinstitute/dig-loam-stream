package loamstream.model.tags.methods

import loamstream.model.tags.piles.LPileTag

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/27/2015.
 */
case class LMethodTag1I1O[I0 <: LPileTag, O0 <: LPileTag](input0: I0, output0: O0)
  extends LMethodTag with LMethodTag.Has1I[I0] with LMethodTag.Has1O[O0] {
  override def inputs: Seq[LPileTag] = Seq(input0)

  override def outputs: Seq[LPileTag] = Seq(output0)

  override def plusKeyI0[KN: TypeTag]=
    LMethodTag1I1O[I0#UpTag[KN], O0#UpTag[KN]](input0.plusKey[KN], output0.plusKey[KN])
}
