package tools.core

import loamstream.model.id.LId
import loamstream.model.id.LId.LNamedId
import loamstream.model.jobs.tools.LTool
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.recipes.LRecipeSpec

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object CoreTool {

  def checkPreExistingVcfFile(id: String): CoreTool =
    CoreTool("What a nice VCF file!", LRecipeSpec.preExistingCheckout(id, CoreStore.vcfFile.pile))

  val extractSampleIdsFromVcfFile =
    CoreTool("Extracted sample ids from VCF file into a text file.",
      LRecipeSpec.keyExtraction(CoreStore.vcfFile.pile, CoreStore.sampleIdsFile.pile,
        PileKinds.sampleKeyIndexInGenotypes))

  val importVcf =
    CoreTool("Import VCF file into VDS format Hail works with.",
      LRecipeSpec.vcfImport(CoreStore.vcfFile.pile, CoreStore.vdsFile.pile, 0))

  val calculateSingletons =
    CoreTool("Calculate singletons from genotype calls in VDS format.",
      LRecipeSpec.calculateSingletons(CoreStore.vdsFile.pile, CoreStore.singletonsFile.pile, 0))

  def tools(vcfFileId: String): Set[LTool] =
    Set[LTool](checkPreExistingVcfFile(vcfFileId), extractSampleIdsFromVcfFile, importVcf, calculateSingletons)

  def apply(name: String, recipe: LRecipeSpec): CoreTool = CoreTool(LNamedId(name), recipe)

}

case class CoreTool(id: LId, recipe: LRecipeSpec) extends LTool
