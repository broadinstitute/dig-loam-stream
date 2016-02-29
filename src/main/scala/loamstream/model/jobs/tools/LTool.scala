package loamstream.model.jobs.tools

import loamstream.model.id.LId
import loamstream.model.recipes.LRecipeSpec

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
trait LTool extends LId.Owner {
  def recipe: LRecipeSpec
}
