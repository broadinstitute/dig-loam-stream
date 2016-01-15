package loamstream.model.calls

import loamstream.model.recipes.{LPileCalls, LRecipe}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */

trait LPileCall[Keys <: LKeys[_, _], Inputs <: LPileCalls[_, _]] {
  def recipe: LRecipe[Inputs]
}
