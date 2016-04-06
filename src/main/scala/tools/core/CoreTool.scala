package tools.core

import loamstream.LEnv
import loamstream.model.id.LId
import loamstream.model.id.LId.LNamedId
import loamstream.model.jobs.tools.LTool
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.kinds.instances.ToolKinds.{klustakwikClustering, nativePcaProjection}
import loamstream.model.recipes.LRecipeSpec
import tools.core.CoreStore.{pcaProjectedFile, pcaWeightsFile, sampleClusterFile, vcfFile}
import tools.core.LCoreEnv.Keys

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object CoreTool {

  def checkPreExistingVcfFile(id: String): CoreTool =
    CoreTool("What a nice VCF file!", LRecipeSpec.preExistingCheckout(id, CoreStore.vcfFile.pile))

  def checkPreExistingPcaWeightsFile(id: String): CoreTool =
    CoreTool("File with PCA weights", LRecipeSpec.preExistingCheckout(id, CoreStore.pcaWeightsFile.pile))

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

  val projectPcaNative = CoreTool("Project PCA using native method",
    LRecipeSpec(nativePcaProjection, Seq(vcfFile.pile, pcaWeightsFile.pile), pcaProjectedFile.pile))

  val klustaKwikClustering = CoreTool("Project PCA using native method",
    LRecipeSpec(klustakwikClustering, Seq(pcaProjectedFile.pile), sampleClusterFile.pile))

  def tools(env: LEnv): Set[LTool] = {
    env.get(Keys.genotypesId).map(checkPreExistingVcfFile(_)).toSet[LTool] ++
      env.get(Keys.pcaWeightsId).map(checkPreExistingPcaWeightsFile(_)).toSet[LTool] ++
      Set[LTool](extractSampleIdsFromVcfFile, importVcf, calculateSingletons, projectPcaNative, klustaKwikClustering)

  }

  def apply(name: String, recipe: LRecipeSpec): CoreTool = CoreTool(LNamedId(name), recipe)

}

case class CoreTool(id: LId, recipe: LRecipeSpec) extends LTool
