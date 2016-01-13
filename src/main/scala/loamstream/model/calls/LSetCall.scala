package loamstream.model.calls

import loamstream.model.calls.props.LProps
import loamstream.model.recipes.{LCheckoutPreexisting, LPileCalls, LRecipe}
import loamstream.model.tags.LTags
import util.Index.{I00, I01}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LSetCall {
  type Set1[K0] = LTags.HasKey[I00, K0]
  type Set2[K0, K1] = Set1[K0] with LTags.HasKey[I01, K1]

  def getPreexisting[Tags <: LTags.IsSet, Props <: LProps](id: String):
  LSetCall[Tags, LPileCalls.LCalls0, Props]
  = LSetCall(new LCheckoutPreexisting(id))
}

case class LSetCall[Tags <: LTags.IsSet, Inputs <: LPileCalls[_, _],
+Props <: LProps](recipe: LRecipe[Inputs])
  extends LPileCall[Tags, Inputs, Props] {
  def withTags[TagsNew <: LTags] = this.asInstanceOf[LSetCall[Tags with TagsNew, Inputs, Props]]

  def withProps[PropsNew <: LProps] = this.asInstanceOf[LSetCall[Tags, Inputs, Props with PropsNew]]
}
