package loamstream.model.jobs.tools

import loamstream.model.recipes.{LRecipe, LRecipeSpec}

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
trait LTool {
  def recipe: LRecipe

  def recipeSpec: LRecipeSpec = recipe.spec
}
