package loamstream.apps.minimal

import loamstream.map.LToolMapping
import loamstream.model.LPipeline
import loamstream.model.jobs.LJob
import loamstream.model.jobs.tools.LTool
import loamstream.model.recipes.LRecipe

import scala.concurrent.ExecutionContext

/**
 * LoamStream
 * Created by oliverr on 2/16/2016.
 */
object MiniTool {

  val checkPreExistingVcfFile =
    MiniTool(LRecipe.preExistingCheckout(MiniPipeline.genotypeCallsPileId, MiniStore.vcfFile.pile),
      "What a nice VCF file!")

  val checkPreExistingGenotypeCassandraTable =
    MiniTool(LRecipe.preExistingCheckout(MiniPipeline.genotypeCallsPileId,
      MiniStore.genotypesCassandraTable.pile),
      "What a nice table on Cassandra full of genotype calls!")

  val extractSampleIdsFromVcfFile =
    MiniTool(LRecipe.keyExtraction(MiniStore.vcfFile.pile, MiniStore.sampleIdsFile.pile, 0),
      "Extracted sample ids from VCF file into a text file.")

  val extractSampleIdsFromCassandraTable =
    MiniTool(LRecipe.keyExtraction(MiniStore.genotypesCassandraTable.pile,
      MiniStore.sampleIdsCassandraTable.pile, 0),
      "Extracted sample ids from Cassandra genotype calls table into another table.")

  val tools = Set[LTool](checkPreExistingVcfFile, checkPreExistingGenotypeCassandraTable, extractSampleIdsFromVcfFile,
    extractSampleIdsFromCassandraTable)

}

case class MiniTool(recipe: LRecipe, comment: String) extends LTool {
  override def createJob(recipe: LRecipe, pipeline: LPipeline, mapping: LToolMapping): LJob = ???
}
