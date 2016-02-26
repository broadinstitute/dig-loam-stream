package loamstream.apps.minimal

import loamstream.model.jobs.tools.LTool
import loamstream.model.recipes.LRecipeSpec

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object MiniTool {

  val checkPreExistingVcfFile =
    MiniTool(LRecipeSpec.preExistingCheckout(MiniPipeline.genotypeCallsPileId, MiniStore.vcfFile.pileSpec),
      "What a nice VCF file!")

  val checkPreExistingGenotypeCassandraTable =
    MiniTool(LRecipeSpec.preExistingCheckout(MiniPipeline.genotypeCallsPileId,
      MiniStore.genotypesCassandraTable.pileSpec),
      "What a nice table on Cassandra full of genotype calls!")

  val extractSampleIdsFromVcfFile =
    MiniTool(LRecipeSpec.keyExtraction(MiniStore.vcfFile.pileSpec, MiniStore.sampleIdsFile.pileSpec, 0),
      "Extracted sample ids from VCF file into a text file.")

  val extractSampleIdsFromCassandraTable =
    MiniTool(LRecipeSpec.keyExtraction(MiniStore.genotypesCassandraTable.pileSpec,
      MiniStore.sampleIdsCassandraTable.pileSpec, 0),
      "Extracted sample ids from Cassandra genotype calls table into another table.")

  val tools = Set[LTool](checkPreExistingVcfFile, checkPreExistingGenotypeCassandraTable, extractSampleIdsFromVcfFile,
    extractSampleIdsFromCassandraTable)

}

case class MiniTool(recipe: LRecipeSpec, comment: String) extends LTool
