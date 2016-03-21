package loamstream.apps.minimal

import loamstream.model.id.LId
import loamstream.model.id.LId.LNamedId
import loamstream.model.jobs.tools.LTool
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.recipes.LRecipeSpec

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object MiniTool {

  val checkPreExistingVcfFile =
    MiniTool("What a nice VCF file!",
      LRecipeSpec.preExistingCheckout(MiniPipeline.genotypeCallsPileId, MiniStore.vcfFile.pile))

  val checkPreExistingGenotypeCassandraTable =
    MiniTool("What a nice table on Cassandra full of genotype calls!",
      LRecipeSpec.preExistingCheckout(MiniPipeline.genotypeCallsPileId,
      MiniStore.genotypesCassandraTable.pile))

  val extractSampleIdsFromVcfFile =
    MiniTool("Extracted sample ids from VCF file into a text file.",
      LRecipeSpec.keyExtraction(MiniStore.vcfFile.pile, MiniStore.sampleIdsFile.pile,
        PileKinds.sampleKeyIndexInGenotypes))

  val extractSampleIdsFromCassandraTable =
    MiniTool("Extracted sample ids from Cassandra genotype calls table into another table.",
      LRecipeSpec.keyExtraction(MiniStore.genotypesCassandraTable.pile,
      MiniStore.sampleIdsCassandraTable.pile, PileKinds.sampleKeyIndexInGenotypes))

  val tools = Set[LTool](checkPreExistingVcfFile, checkPreExistingGenotypeCassandraTable, extractSampleIdsFromVcfFile,
    extractSampleIdsFromCassandraTable)

  def apply(name: String, recipe: LRecipeSpec): MiniTool = MiniTool(LNamedId(name), recipe)

}

case class MiniTool(id: LId, recipe: LRecipeSpec) extends LTool
