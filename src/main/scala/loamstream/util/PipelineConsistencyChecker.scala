package loamstream.util

import loamstream.model.LPipeline
import loamstream.model.Store
import loamstream.model.ToolBase

/**
  * LoamStream
  * Created by oliverr on 3/24/2016.
  */
object PipelineConsistencyChecker {

  sealed trait Problem {
    def message: String
  }

  sealed trait Check extends (LPipeline => Set[Problem])

  case object NoPiles extends Problem {
    override def message: String = "Pipeline contains no piles."
  }

  case object PipelineHasPilesCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] =
      if (pipeline.piles.isEmpty) Set(NoPiles) else Set.empty
  }

  case object NoRecipes extends Problem {
    override def message: String = "Pipeline contains no recipes"
  }

  case object PipelineHasRecipesCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] =
      if (pipeline.recipes.isEmpty) Set(NoRecipes) else Set.empty
  }

  sealed trait PileSpecificProblem extends Problem {
    def pile: Store
  }

  sealed trait RecipeSpecificProblem extends Problem {
    def recipe: ToolBase
  }

  sealed trait PileIsNotProducedByExactlyOneRecipe extends PileSpecificProblem

  case class PileIsProducedByNoRecipe(pile: Store) extends PileIsNotProducedByExactlyOneRecipe {
    override def message: String = s"Pile ${pile.id} is not produced by any recipe."
  }

  case object EachPileIsOutputOfAtLeastOneRecipeCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      (pipeline.piles -- pipeline.recipes.map(_.output)).map(PileIsProducedByNoRecipe)
    }
  }

  case class PileIsProducedByMultipleRecipes(pile: Store, recipes: Set[ToolBase])
    extends PileIsNotProducedByExactlyOneRecipe {
    override def message: String =
      s"Pile ${pile.id} is produced by multiple recipes: ${recipes.map(_.id).mkString(", ")}."
  }

  case object EachPileIsOutputOfNoMoreThanOneRecipeCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.recipes.groupBy(_.output).collect { case (pile, recipes) if recipes.size > 1 =>
        val result: Problem = PileIsProducedByMultipleRecipes(pile, recipes)
        
        result
      }.toSet[Problem]
    }
  }

  sealed trait PileIsNotCompatibleWithRecipe extends PileSpecificProblem with RecipeSpecificProblem

  case class PileIsIncompatibleOutputOfRecipe(pile: Store, recipe: ToolBase) extends PileIsNotCompatibleWithRecipe {
    override def message: String = s"Pile ${pile.id} is not compatible output of recipe ${recipe.id}."
  }

  case object EachPileIsCompatibleOutputOfRecipeCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.recipes.filterNot(recipe => recipe.output.spec >:> recipe.spec.output).
        map(recipe => PileIsIncompatibleOutputOfRecipe(recipe.output, recipe))
    }
  }

  case class PileIsIncompatibleInputOfRecipe(pile: Store, recipe: ToolBase, pos: Int)
    extends PileIsNotCompatibleWithRecipe {
    override def message: String =
      s"Pile ${pile.id} is not compatible input (position $pos) of recipe ${recipe.id}."
  }

  case object EachPileIsCompatibleInputOfRecipeCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.recipes.flatMap { recipe =>
        recipe.inputs.indices.map(pos => (recipe, pos, recipe.inputs(pos), recipe.spec.inputs(pos)))
      }.collect { case (recipe, pos, input, inputSpec) if !(input.spec <:< inputSpec) =>
        PileIsIncompatibleInputOfRecipe(input, recipe, pos)
      }
    }
  }

  sealed trait PileMissingUsedInRecipe extends PileSpecificProblem with RecipeSpecificProblem

  case class PileMissingUsedAsOutput(pile: Store, recipe: ToolBase) extends PileMissingUsedInRecipe {
    override def message: String = s"Pile ${pile.id} used as output in recipe ${recipe.id} is missing."
  }

  case object EachOutputPileIsPresentCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.recipes.collect { case recipe if !pipeline.piles.contains(recipe.output) =>
        PileMissingUsedAsOutput(recipe.output, recipe)
      }
    }
  }

  case class PileMissingUsedAsInput(pile: Store, recipe: ToolBase, pos: Int) extends PileMissingUsedInRecipe {
    override def message: String = s"Pile ${pile.id} used as input (pos $pos) in recipe ${recipe.id} is missing."
  }

  case object EachInputPileIsPresentCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.recipes.flatMap { recipe => recipe.inputs.indices.map(pos => (recipe.inputs(pos), recipe, pos)) }.
        collect { case (input, recipe, pos) if !pipeline.piles.contains(input) =>
          PileMissingUsedAsInput(input, recipe, pos)
        }
    }
  }

  case class PipelineIsDisconnected(pile: Store, otherPile: Store) extends PileSpecificProblem {
    override def message: String = s"Pipeline is disconnected: no path from pile ${pile.id}  to ${otherPile.id}."
  }

  case object ConnectednessCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      pipeline.piles.headOption match {
        case Some(arbitraryPile) =>
          var makingProgress = true
          var connectedPiles: Set[Store] = Set(arbitraryPile)
          while (makingProgress) {
            makingProgress = false
            for (recipe <- pipeline.recipes) {
              val neighborPiles = recipe.inputs.toSet + recipe.output
              val connectedPilesNew = neighborPiles -- connectedPiles
              if (connectedPilesNew.nonEmpty) {
                connectedPiles ++= connectedPilesNew
                makingProgress = true
              }
            }
          }
          val otherPiles = pipeline.piles -- connectedPiles
          if (otherPiles.nonEmpty) Set(PipelineIsDisconnected(arbitraryPile, otherPiles.head)) else Set.empty
        case None => Set.empty
      }
    }
  }

  case class PipelineHasCycle(pile: Store) extends PileSpecificProblem {
    override def message: String = s"Pipeline contains a cycle containing pile ${pile.id}."
  }

  case object AcyclicityCheck extends Check {
    override def apply(pipeline: LPipeline): Set[Problem] = {
      //TODO: Get rid of vars with a fold 
      var pilesLeft = pipeline.piles
      var makingProgress = true
      var nPilesLeft = pilesLeft.size
      while (pilesLeft.nonEmpty && makingProgress) {
        pilesLeft = pipeline.recipes.filter(recipe => pilesLeft.contains(recipe.output)).flatMap(_.inputs)
        val nPilesLeftNew = pilesLeft.size
        makingProgress = nPilesLeftNew < nPilesLeft
        nPilesLeft = nPilesLeftNew
      }
      if (pilesLeft.nonEmpty) Set(PipelineHasCycle(pilesLeft.head)) else Set.empty
    }
  }

  val allChecks: Set[Check] = {
    Set(PipelineHasPilesCheck, PipelineHasRecipesCheck, EachPileIsOutputOfAtLeastOneRecipeCheck,
      EachPileIsOutputOfNoMoreThanOneRecipeCheck, EachPileIsCompatibleOutputOfRecipeCheck,
      EachPileIsCompatibleInputOfRecipeCheck, EachOutputPileIsPresentCheck, EachInputPileIsPresentCheck,
      ConnectednessCheck, AcyclicityCheck)
  }

  def check(pipeline: LPipeline, checks: Set[Check] = allChecks): Set[Problem] = {
    checks.flatMap(check => check(pipeline))
  }
}
