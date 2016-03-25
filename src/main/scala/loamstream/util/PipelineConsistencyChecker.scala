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

  case object NoPiles extends Problem {
    override def message: String = "Pipeline contains no piles."
  }

  case object NoRecipes extends Problem {
    override def message: String = "Pipeline contains no recipes"
  }

  sealed trait PileSpecificProblem extends Problem {
    def pile: LPile
  }

  sealed trait RecipeSpecificProblem extends Problem {
    def recipe: LRecipe
  }

  sealed trait PileIsNotProducedByExactlyOneRecipe extends PileSpecificProblem

  case class PileIsProducedByNoRecipe(pile: LPile) extends PileIsNotProducedByExactlyOneRecipe {
    override def message: String = "Pile " + pile.id + " is not produced by any recipe."
  }

  case class PileIsProducedByMultipleRecipes(pile: LPile, recipes: Set[LRecipe])
    extends PileIsNotProducedByExactlyOneRecipe {
    override def message: String =
      "Pile " + pile.id + " is produced by multiple recipes: " + recipes.map(_.id).mkString(", ") + "."
  }

  sealed trait PileIsNotCompatibleWithRecipe extends PileSpecificProblem with RecipeSpecificProblem

  case class PileIsIncompatibleOutputOfRecipe(pile: LPile, recipe: LRecipe) extends PileIsNotCompatibleWithRecipe {
    override def message: String = "Pile " + pile.id + " is not compatible output of recipe " + recipe.id + "."
  }

  case class PileIsIncompatibleInputOfRecipe(pile: LPile, recipe: LRecipe, pos: Int)
    extends PileIsNotCompatibleWithRecipe {
    override def message: String =
      "Pile " + pile.id + " is not compatible input (position " + pos + ") of recipe " + recipe.id + "."
  }

  sealed trait PileMissingUsedInRecipe extends PileSpecificProblem with RecipeSpecificProblem

  case class PileMissingUsedAsOutput(pile: LPile, recipe: LRecipe) extends PileMissingUsedInRecipe {
    override def message: String = "Pile " + pile.id + " used as output in recipe " + recipe.id + " is missing."
  }

  case class PileMissingUsedAsInput(pile: LPile, recipe: LRecipe, pos: Int) extends PileMissingUsedInRecipe {
    override def message: String = "Pile " + pile.id + " used as input (pos " + pos + ") in recipe " +
      recipe.id + " is missing."
  }

  def checkPipelineHasPilesAndRecipes(pipeline: LPipeline): Seq[Problem] = {
    (pipeline.piles.isEmpty, pipeline.recipes.isEmpty) match {
      case (true, true) => Seq(NoPiles, NoRecipes)
      case (true, false) => Seq(NoPiles)
      case (false, true) => Seq(NoRecipes)
      case (false, false) => Seq.empty
    }
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

  def checkEachOutputIsPresent(pipeline: LPipeline): Iterable[PileMissingUsedAsOutput] = {
    pipeline.recipes.collect({ case recipe if !pipeline.piles.contains(recipe.output) =>
      PileMissingUsedAsOutput(recipe.output, recipe)
    })
  }

  def checkEachInputIsPresent(pipeline: LPipeline): Iterable[PileMissingUsedAsInput] = {
    pipeline.recipes.flatMap({ recipe => recipe.inputs.indices.map(pos => (recipe.inputs(pos), recipe, pos)) }).
      collect({ case (input, recipe, pos) if !pipeline.piles.contains(input) =>
        PileMissingUsedAsInput(input, recipe, pos)
      })
  }

  def check(pipeline: LPipeline): Iterable[Problem] = {
    checkPipelineHasPilesAndRecipes(pipeline) ++ checkEachPileIsProducedByExactlyOneRecipe(pipeline) ++
      checkEachOutputPileIsCompatible(pipeline) ++ checkEachInputPileIsCompatible(pipeline) ++
      checkEachOutputIsPresent(pipeline) ++ checkEachInputIsPresent(pipeline)
  }

}
