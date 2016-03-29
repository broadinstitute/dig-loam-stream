package loamstream.apps.minimal

import loamstream.model.id.LId
import loamstream.model.id.LId.LNamedId
import loamstream.model.jobs.tools.LTool
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.recipes.LRecipeSpec
import tools.core.CoreStore

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object MiniTool {

  val checkPreExistingVcfFile =
    MiniTool("What a nice VCF file!",
      LRecipeSpec.preExistingCheckout(MiniPipeline.genotypeCallsPileId, CoreStore.vcfFile.pile))

  val checkPreExistingGenotypeCassandraTable =
    MiniTool("What a nice table on Cassandra full of genotype calls!",
      LRecipeSpec.preExistingCheckout(MiniPipeline.genotypeCallsPileId,
      MiniMockStore.genotypesCassandraTable.pile))

  val extractSampleIdsFromVcfFile =
    MiniTool("Extracted sample ids from VCF file into a text file.",
      LRecipeSpec.keyExtraction(CoreStore.vcfFile.pile, CoreStore.sampleIdsFile.pile,
        PileKinds.sampleKeyIndexInGenotypes))

  val extractSampleIdsFromCassandraTable =
    MiniTool("Extracted sample ids from Cassandra genotype calls table into another table.",
      LRecipeSpec.keyExtraction(MiniMockStore.genotypesCassandraTable.pile,
      MiniMockStore.sampleIdsCassandraTable.pile, PileKinds.sampleKeyIndexInGenotypes))

  val importVcf =
    MiniTool("Import VCF file into VDS format Hail works with.",
      LRecipeSpec.vcfImport(CoreStore.vcfFile.pile, CoreStore.vdsFile.pile, 0))

  val calculateSingletons =
    MiniTool("Calculate singletons from genotype calls in VDS format.",
      LRecipeSpec.calculateSingletons(CoreStore.vdsFile.pile, CoreStore.singletonsFile.pile, 0))

  val tools = Set[LTool](checkPreExistingVcfFile, checkPreExistingGenotypeCassandraTable, extractSampleIdsFromVcfFile,
    extractSampleIdsFromCassandraTable, importVcf, calculateSingletons)

  def apply(name: String, recipe: LRecipeSpec): MiniTool = MiniTool(LNamedId(name), recipe)

}

case class MiniTool(id: LId, recipe: LRecipeSpec) extends LTool
