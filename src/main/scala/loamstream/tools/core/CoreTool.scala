package loamstream.tools.core

import loamstream.LEnv
import loamstream.model.id.LId
import loamstream.model.id.LId.LNamedId
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.kinds.instances.ToolKinds.{klustakwikClustering, nativePcaProjection}
import loamstream.model.recipes.LRecipeSpec
import LCoreEnv._
import loamstream.model.ToolBase
import loamstream.model.Store
import loamstream.model.piles.LPileSpec
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
case class CoreTool(id: LId, spec: LRecipeSpec, inputs: Seq[Store], output: Store) extends ToolBase

object CoreTool {
  
  def apply(name: String, recipe: LRecipeSpec, inputs: Seq[Store], output: Store): CoreTool = CoreTool(LNamedId(name), recipe, inputs, output)
  
  import StoreOps._
  
  def checkPreExistingVcfFile(id: String): ToolBase = nullaryTool(
      id, 
      "What a nice VCF file!",
      CoreStore.vcfFile,
      LRecipeSpec.preExistingCheckout(id))

  def checkPreExistingPcaWeightsFile(id: String): ToolBase = nullaryTool(
      id, 
      "File with PCA weights",
      CoreStore.pcaWeightsFile,
      LRecipeSpec.preExistingCheckout(id))

  val extractSampleIdsFromVcfFile: ToolBase = unaryTool(
      "Extracted sample ids from VCF file into a text file.",
      CoreStore.vcfFile ~> CoreStore.sampleIdsFile,
      LRecipeSpec.keyExtraction(PileKinds.sampleKeyIndexInGenotypes) _)

  val importVcf: ToolBase = unaryTool(
      "Import VCF file into VDS format Hail works with.",
      CoreStore.vcfFile ~> CoreStore.vdsFile,
      LRecipeSpec.vcfImport(0) _)

  val calculateSingletons: ToolBase = unaryTool(
      "Calculate singletons from genotype calls in VDS format.",
      CoreStore.vdsFile ~> CoreStore.singletonsFile,
      LRecipeSpec.calculateSingletons(0) _)

  val projectPcaNative: ToolBase = binaryTool(
      "Project PCA using native method", 
      nativePcaProjection,
      (CoreStore.vcfFile, CoreStore.pcaWeightsFile) ~> CoreStore.pcaProjectedFile)

  val klustaKwikClustering: ToolBase = unaryTool(
      "Project PCA using native method", 
      klustakwikClustering,
      CoreStore.pcaProjectedFile ~> CoreStore.sampleClusterFile)
  
  def tools(env: LEnv): Set[ToolBase] = {
    env.get(Keys.genotypesId).map(checkPreExistingVcfFile(_)).toSet ++
    env.get(Keys.pcaWeightsId).map(checkPreExistingPcaWeightsFile(_)).toSet ++
    Set(extractSampleIdsFromVcfFile, importVcf, calculateSingletons, projectPcaNative, klustaKwikClustering)
  }
  
  //TODO: TEST
  def nullaryTool(id: String, name: String, output: Store, makeRecipeSpec: LPileSpec => LRecipeSpec): ToolBase = {
    CoreTool(name, makeRecipeSpec(output.spec), Seq.empty, output)
  }
  
  //TODO: TEST
  def unaryTool(name: String, sig: UnarySig, makeRecipeSpec: (LPileSpec, LPileSpec) => LRecipeSpec): ToolBase = {
    CoreTool(
      name,
      makeRecipeSpec(sig.input.spec, sig.output.spec),
      Seq(sig.input),
      sig.output)
  }
  
  //TODO: TEST
  def unaryTool(name: String, kind: LKind, sig: UnarySig): ToolBase = nAryTool(name, kind, sig.toNarySig)
  
  //TODO: TEST
  def binaryTool(name: String, kind: LKind, sig: BinarySig): ToolBase = nAryTool(name, kind, sig.toNarySig)
  
  //TODO: TEST
  def nAryTool(name: String, kind: LKind, sig: NarySig): ToolBase = nAryTool(LId.LNamedId(name), kind, sig)
  
  //TODO: TEST
  def nAryTool(kind: LKind, sig: NarySig): ToolBase = nAryTool(LId.newAnonId, kind, sig)
  
  //TODO: TEST
  def nAryTool(id: LId, kind: LKind, sig: NarySig): ToolBase = {
    CoreTool(
      id,
      LRecipeSpec(kind, sig.inputs.map(_.spec), sig.output.spec),
      sig.inputs,
      sig.output)
  }
}
