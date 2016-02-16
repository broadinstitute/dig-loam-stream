package loamstream.model.jobs.tools

import loamstream.model.jobs.LJob
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
trait LTool[T] {

  def recipe: LRecipe

  def createJob(inputTools: Seq[LTool[_]]): LJob[T]

}
