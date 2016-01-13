package loamstream.model.calls

import loamstream.model.calls.props.LProps
import loamstream.model.recipes.{LPileCalls, LRecipe}
import loamstream.model.tags.LTags

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LMapCall {
  //  def getPreexisting[Tag <: LMapTag[_, _, _], Props <: LProps](tag: Tag, id: String):
  //  LMapCall[Tag, LPileCalls.LCalls0, Props]
  //  = LMapCall(tag, new LCheckoutPreexisting(id))
}

case class LMapCall[Tags <: LTags.IsMap, Inputs <: LPileCalls[_, _],
+Props <: LProps](recipe: LRecipe[Inputs])
  extends LPileCall[Tags, Inputs, Props] {
  def withTags[TagsNew <: LTags] = this.asInstanceOf[LMapCall[Tags with TagsNew, Inputs, Props]]

  def withProps[PropsNew <: LProps] = this.asInstanceOf[LMapCall[Tags, Inputs, Props with PropsNew]]
}
