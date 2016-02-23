package loamstream.model.jobs.tools

import loamstream.map.LToolMapping
import loamstream.model.jobs.LJob
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
trait LTool {

  def recipe: LRecipe

  def createJob(recipe: LRecipe, mapping:LToolMapping): LJob

}
