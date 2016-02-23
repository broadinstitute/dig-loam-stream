package loamstream.apps.minimal

import java.nio.file.Path

import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/23/2016.
  */
object MiniToolBox extends LBasicToolBox {
  val stores = MiniMockStore.stores
  val tools = MiniMockTool.tools

  override def storesFor(pile: LPile): Set[LStore] = stores.filter(_.pile <:< pile)

  override def toolsFor(recipe: LRecipe): Set[LTool] = tools.filter(_.recipe <<< recipe)

  override def getPredefindedVcfFile(id: String): Path = ???

  override def pickNewSampleFile: Path = ???

}
