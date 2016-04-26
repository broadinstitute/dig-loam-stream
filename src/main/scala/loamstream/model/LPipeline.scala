package loamstream.model

import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object LPipeline {

  case class Flat(piles: Set[LPile], recipes: Set[LRecipe]) extends LPipeline

}

trait LPipeline {
  def piles: Set[LPile]

  def recipes: Set[LRecipe]
}
