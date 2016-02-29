package loamstream.model

import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object LPipeline {
  def apply(pile: LPile, piles: LPile*)(recipe: LRecipe, recipes: LRecipe*): LPipeline =
    LPipeline((pile +: piles).toSet, (recipe +: recipes).toSet)

}

case class LPipeline(piles: Set[LPile], recipes: Set[LRecipe]) {

}
