package loamstream.model.jobs.tools

import loamstream.model.jobs.LJob
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
trait LTool {

  def recipe: LRecipe

  def createJob[T](inputTools: Seq[LTool]): LJob[T]

}
