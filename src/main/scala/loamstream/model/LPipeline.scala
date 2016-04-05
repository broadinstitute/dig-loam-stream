package loamstream.model

import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
trait LPipeline {
  def piles: Set[LPile]

  def recipes: Set[LRecipe]
}
