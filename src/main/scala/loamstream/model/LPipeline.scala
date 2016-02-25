package loamstream.model

import loamstream.model.calls.LPileCall
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe

/**
 * LoamStream
 * Created by oliverr on 2/17/2016.
 */
object LPipeline {
  def apply(calls: LPileCall*): LPipeline = LPipeline(calls.toSet)
}

case class LPipeline(calls: Set[LPileCall]) {

  def piles: Set[LPile] = calls.map(_.pile)
  def recipes: Set[LRecipe] = calls.map(_.recipe)
}
