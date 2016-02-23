package loamstream.apps.minimal

import java.nio.file.Path

import loamstream.apps.minimal.MiniToolBox.Config
import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/23/2016.
  */
object MiniToolBox {

  case class Config(dataFilesFolder: Path)

}

case class MiniToolBox(config: Config) extends LBasicToolBox {
  val vcfFilesFolder = config.dataFilesFolder.resolve("vcf")
  val sampleFilesFolder = config.dataFilesFolder.resolve("samples")
  val stores = MiniStore.stores
  val tools = MiniTool.tools

  override def storesFor(pile: LPile): Set[LStore] = stores.filter(_.pile <:< pile)

  override def toolsFor(recipe: LRecipe): Set[LTool] = tools.filter(_.recipe <<< recipe)

  override def getPredefindedVcfFile(id: String): Path = vcfFilesFolder.resolve(id + ".vcf")

  override def pickNewSampleFile: Path = sampleFilesFolder.resolve("samples" + System.currentTimeMillis())

}
