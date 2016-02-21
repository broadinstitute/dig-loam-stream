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
    MiniMockTool(LRecipe.preExistingCheckout(MiniPipeline.genotypeCallsPileId, MiniMockStore.vcfFile.pile),
      "What a nice VCF file!")

  val checkPreExistingGenotypeCassandraTable =
    MiniMockTool(LRecipe.preExistingCheckout(MiniPipeline.genotypeCallsPileId,
      MiniMockStore.genotypesCassandraTable.pile),
      "What a nice table on Cassandra full of genotype calls!")

  val extractSampleIdsFromVcfFile =
    MiniMockTool(LRecipe.keyExtraction(MiniMockStore.vcfFile.pile, MiniMockStore.sampleIdsFile.pile, 0),
      "Extracted sample ids from VCF file into a text file.")

  val extractSampleIdsFromCassandraTable =
    MiniMockTool(LRecipe.keyExtraction(MiniMockStore.genotypesCassandraTable.pile,
      MiniMockStore.sampleIdsCassandraTable.pile, 0),
      "Extracted sample ids from Cassandra genotype calls table into another table.")

  val tools = Set[LTool](checkPreExistingVcfFile, checkPreExistingGenotypeCassandraTable, extractSampleIdsFromVcfFile,
    extractSampleIdsFromCassandraTable)

}

case class MiniMockTool(recipe: LRecipe, comment: String) extends LTool {
  override def createJob[T](inputTools: Seq[LTool]): LJob[T] = ???
}
