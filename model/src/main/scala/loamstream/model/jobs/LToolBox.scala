package loamstream.model.jobs

import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore

/**
 * LoamStream
 * Created by oliverr on 2/16/2016.
 */
object LToolBox {

  case class LToolBag(stores: Set[LStore], tools: Set[LTool]) extends LToolBox {
    override def storesFor(pile: LPile): Set[LStore] = stores.filter(_.pile <:< pile)

    override def toolsFor(recipe: LRecipe): Set[LTool] = tools.filter(_.recipe <<< recipe)
  }

}

trait LToolBox {
  def storesFor(pile: LPile): Set[LStore]

  def toolsFor(recipe: LRecipe): Set[LTool]
}
