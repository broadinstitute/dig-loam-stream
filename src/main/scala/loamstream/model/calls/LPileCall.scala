package loamstream.model.calls

import loamstream.model.calls.props.LProps
import loamstream.model.recipes.{LPileCalls, LRecipe}
import loamstream.model.tags.LPileTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LPileCall[Tag <: LPileTag[_, _], Inputs <: LPileCalls[_, _], +Props <: LProps] {
  type MyTag = Tag
  type MyInputs = Inputs
  type MyProps = Props

  def tag: Tag

  def recipe: LRecipe[Inputs]

  def withProps[PropsNew <: LProps]: LPileCall[Tag, Inputs, Props with PropsNew]
}
