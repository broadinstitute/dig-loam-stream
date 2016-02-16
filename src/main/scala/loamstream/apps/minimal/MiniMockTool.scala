package loamstream.apps.minimal

import loamstream.model.jobs.LJob
import loamstream.model.jobs.tools.LTool
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object MiniMockTool {

  val checkPreExistingVcfFile =
    MiniMockTool(LRecipe.preExistingCheckout(MiniApp.genotypeCallsPileId, MiniMockStores.vcfFile), "What a nice VCF file!")

}

case class MiniMockTool[T](recipe: LRecipe, comment: String) extends LTool[T] {
  override def createJob(inputTools: Seq[LTool[_]]): LJob[T] = ???
}
