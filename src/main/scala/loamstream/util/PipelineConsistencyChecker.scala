package loamstream.util

import loamstream.model.LPipeline
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 3/24/2016.
  */
object PipelineConsistencyChecker {

  sealed trait Problem {
    def message: String
  }

  sealed trait PileIsNotProducedByExactlyOneRecipe extends Problem {
    def pile: LPile
  }

  case class PileIsProducedByNoRecipe(pile: LPile) extends PileIsNotProducedByExactlyOneRecipe {
    override def message: String = "Pile " + pile.id + " is not produced by any recipe."
  }

  case class PileIsProducedByMultipleRecipes(pile: LPile, recipes: Set[LRecipe])
    extends PileIsNotProducedByExactlyOneRecipe {
    override def message: String =
      "Pile " + pile.id + " is produced by multiple recipes: " + recipes.map(_.id).mkString(", ") + "."
  }

  sealed trait PileIsNotCompatibleWithRecipe extends Problem {
    def pile: LPile

    def recipe: LRecipe
  }

  case class PileIsIncompatibleOutputOfRecipe(pile: LPile, recipe: LRecipe) extends PileIsNotCompatibleWithRecipe {
    override def message: String = "Pile " + pile.id + " is not compatible output of recipe " + recipe.id + "."
  }

  case class PileIsIncompatibleInputOfRecipe(pile: LPile, recipe: LRecipe, pos: Int)
    extends PileIsNotCompatibleWithRecipe {
    override def message: String =
      "Pile " + pile.id + " is not compatible input (position " + pos + ") of recipe " + recipe.id + "."
  }

  def checkEachPileIsProducedByExactlyOneRecipe(pipeline: LPipeline): Iterable[PileIsNotProducedByExactlyOneRecipe] = {
    val pilesProducedByNoRecipe = (pipeline.piles -- pipeline.recipes.map(_.output)).map(PileIsProducedByNoRecipe)
    val pilesProducedByMultipleRecipes =
      pipeline.recipes.groupBy(_.output).collect({ case (pile, recipes) if recipes.size > 1 =>
        PileIsProducedByMultipleRecipes(pile, recipes)
      })
    pilesProducedByNoRecipe ++ pilesProducedByMultipleRecipes
  }

  def checkEachOutputPileIsCompatible(pipeline: LPipeline): Iterable[PileIsIncompatibleOutputOfRecipe] = {
    pipeline.recipes.filterNot(recipe => recipe.output.spec >:> recipe.spec.output).
      map(recipe => PileIsIncompatibleOutputOfRecipe(recipe.output, recipe))
  }

  def checkEachInputPileIsCompatible(pipeline: LPipeline): Iterable[PileIsIncompatibleInputOfRecipe] = {
    pipeline.recipes.flatMap({ recipe =>
      recipe.inputs.indices.map(pos => (recipe, pos, recipe.inputs(pos), recipe.spec.inputs(pos)))
    }).collect({ case (recipe, pos, input, inputSpec) if !(input.spec <:< inputSpec) =>
      PileIsIncompatibleInputOfRecipe(input, recipe, pos)
    })
  }

  def check(pipeline: LPipeline): Iterable[Problem] = {
    checkEachPileIsProducedByExactlyOneRecipe(pipeline) ++ checkEachOutputPileIsCompatible(pipeline) ++
      checkEachInputPileIsCompatible(pipeline)
  }

}
