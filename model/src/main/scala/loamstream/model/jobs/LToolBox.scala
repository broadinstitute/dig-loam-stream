package loamstream.model.jobs

import loamstream.model.jobs.tools.LTool
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object LToolBox {

  case class LToolBag(tools: Set[LTool[_]]) extends LToolBox {
    override def toolsFor(recipe: LRecipe): Set[LTool[_]] = tools.filter(_.recipe <:< recipe)
  }

}

trait LToolBox {
  def toolsFor(recipe: LRecipe): Set[LTool[_]]
}
