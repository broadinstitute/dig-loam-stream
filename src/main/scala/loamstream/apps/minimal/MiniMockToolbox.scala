package loamstream.apps.minimal

import loamstream.LEnv
import loamstream.map.LToolMapping
import loamstream.model.LPipeline
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.tools.LTool
import loamstream.model.jobs.{LJob, LToolBox}
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore
import loamstream.util.shot.{Hit, Miss, Shot}
import loamstream.util.snag.SnagMessage
import tools.core.{LCoreDefaultPileIds, LCoreEnv}

/**
  * LoamStream
  * Created by oliverr on 3/31/2016.
  */
object MiniMockToolBox {
  def apply(env: LEnv): Shot[MiniMockToolBox] = {
    env.get(LCoreEnv.Keys.genotypesId) match {
      case Some(genotypesId) => Hit(MiniMockToolBox(genotypesId))
      case None => Miss("Genotypes id not defined in environment.")
    }
  }
}

case class MiniMockToolBox(genotypesId: String = LCoreDefaultPileIds.genotypes) extends LToolBox {
  val stores = MiniMockStore.stores
  val tools = MiniMockTool.tools(genotypesId)

  override def storesFor(pile: LPile): Set[LStore] = stores.filter(_.pile <:< pile.spec)

  override def toolsFor(recipe: LRecipe): Set[LTool] = tools.filter(_.recipe <<< recipe.spec)

  override def createJobs(recipe: LRecipe, pipeline: LPipeline, mapping: LToolMapping): Shot[Set[LJob]] = {
    mapping.tools.get(recipe) match {
      case Some(tool) => tool match {
        case _ => Miss(SnagMessage("Have not yet implemented tool " + tool))
      }
      case None => Miss(SnagMessage("No tool mapped to recipe " + recipe))
    }
  }

  override def createExecutable(pipeline: LPipeline, mapping: LToolMapping): LExecutable = {
    LExecutable(mapping.tools.keySet.map(createJobs(_, pipeline, mapping)).collect({ case Hit(job) => job }).flatten)
  }
}
