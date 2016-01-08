package loamstream.model.calls

import loamstream.model.calls.props.LProps
import loamstream.model.recipes.{LCheckoutPreexisting, LPileCalls, LRecipe}
import loamstream.model.tags.LSetTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LSetCall {
  def getPreexisting[Tag <: LSetTag[_, _], Props <: LProps](tag: Tag, id: String):
  LSetCall[Tag, LPileCalls.LCalls0, Props]
  = LSetCall(tag, new LCheckoutPreexisting(id))
}

case class LSetCall[Tag <: LSetTag[_, _], Inputs <: LPileCalls[_, _, _],
+Props <: LProps](tag: Tag, recipe: LRecipe[Inputs])
  extends LPileCall[Tag, Inputs, Props] {

}
