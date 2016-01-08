package loamstream.model.calls

import loamstream.model.calls.props.LProps
import loamstream.model.recipes.{LCheckoutPreexisting, LPileCalls, LRecipe}
import loamstream.model.tags.LMapTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LMapCall {
  def getPreexisting[Tag <: LMapTag[_, _, _], Props <: LProps](tag: Tag, id: String):
  LMapCall[Tag, LPileCalls.LCalls0, Props]
  = LMapCall(tag, new LCheckoutPreexisting(id))
}

case class LMapCall[Tag <: LMapTag[_, _, _], Inputs <: LPileCalls[_, _, _],
+Props <: LProps](tag: Tag, recipe: LRecipe[Inputs])
  extends LPileCall[Tag, Inputs, Props] {
  def withProps[PropsNew <: LProps] = this.asInstanceOf[LMapCall[Tag, Inputs, Props with PropsNew]]
}
