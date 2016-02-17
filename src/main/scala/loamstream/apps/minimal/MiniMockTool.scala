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
    MiniMockTool(LRecipe.preExistingCheckout(MiniPipeline.genotypeCallsPileId, MiniMockStores.vcfFile),
      "What a nice VCF file!")

  val checkPreExistingGenotypeCassandraTable =
    MiniMockTool(LRecipe.preExistingCheckout(MiniPipeline.genotypeCallsPileId, MiniMockStores.genotypesCassandraTable),
      "What a nice table on Cassandra full of genotype calls!")

  val extractSampleIdsFromVcfFile =
    MiniMockTool(LRecipe.keyExtraction(MiniPipeline.genotypeCallsPile, MiniMockStores.sampleIdsFile, 0),
      "Extracted sample ids from VCF file into a text file.")

  val extractSampleIdsFromCassandraTable =
    MiniMockTool(LRecipe.keyExtraction(MiniPipeline.genotypeCallsPile, MiniMockStores.sampleIdsCassandraTable, 0),
      "Extracted sample ids from Cassandra genotype calls table into another table.")

}

case class MiniMockTool[T](recipe: LRecipe, comment: String) extends LTool[T] {
  override def createJob(inputTools: Seq[LTool[_]]): LJob[T] = ???
}
