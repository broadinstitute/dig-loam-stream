package loamstream.model.jobs

import loamstream.map.LToolMapping
import loamstream.model.LPipeline
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore
import loamstream.util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
trait LToolBox {
  def storesFor(pile: LPile): Set[LStore]

  def toolsFor(recipe: LRecipe): Set[LTool]

  def createJobs(recipe: LRecipe, pipeline: LPipeline, mapping: LToolMapping): Shot[Set[LJob]]

  def createExecutable(pipeline: LPipeline, mapping: LToolMapping): LExecutable

}
