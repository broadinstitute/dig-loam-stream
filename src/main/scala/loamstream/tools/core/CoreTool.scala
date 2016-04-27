package loamstream.tools.core

import loamstream.LEnv
import loamstream.model.id.LId
import loamstream.model.id.LId.LNamedId
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.kinds.instances.ToolKinds.{klustakwikClustering, nativePcaProjection, pcaProjection}
import loamstream.model.recipes.LRecipeSpec
import LCoreEnv._
import loamstream.model.Tool
import loamstream.model.Store
import loamstream.model.piles.LPileSpec
import loamstream.model.kinds.LKind
import loamstream.model.kinds.instances.ToolKinds

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
case class CoreTool(id: LId, spec: LRecipeSpec, inputs: Seq[Store], output: Store) extends Tool

object CoreTool {
  
  def apply(name: String, recipe: LRecipeSpec, inputs: Seq[Store], output: Store): CoreTool = CoreTool(LNamedId(name), recipe, inputs, output)
  
  import StoreOps._
  
  def checkPreExistingVcfFile(id: String): Tool = nullaryTool(
      id, 
      "What a nice VCF file!",
      CoreStore.vcfFile,
      LRecipeSpec.preExistingCheckout(id))

  def checkPreExistingPcaWeightsFile(id: String): Tool = nullaryTool(
      id, 
      "File with PCA weights",
      CoreStore.pcaWeightsFile,
      LRecipeSpec.preExistingCheckout(id))

  val extractSampleIdsFromVcfFile: Tool = unaryTool(
      "Extracted sample ids from VCF file into a text file.",
      CoreStore.vcfFile ~> CoreStore.sampleIdsFile,
      LRecipeSpec.keyExtraction(PileKinds.sampleKeyIndexInGenotypes) _)

  val importVcf: Tool = unaryTool(
      "Import VCF file into VDS format Hail works with.",
      CoreStore.vcfFile ~> CoreStore.vdsFile,
      LRecipeSpec.vcfImport(0) _)

  val calculateSingletons: Tool = unaryTool(
      "Calculate singletons from genotype calls in VDS format.",
      CoreStore.vdsFile ~> CoreStore.singletonsFile,
      LRecipeSpec.calculateSingletons(0) _)

  val projectPcaNative: Tool = binaryTool(
      "Project PCA using native method", 
      nativePcaProjection,
      (CoreStore.vcfFile, CoreStore.pcaWeightsFile) ~> CoreStore.pcaProjectedFile)
      
  val projectPca: Tool = binaryTool(
      "Project PCA", 
      pcaProjection,
       (CoreStore.vcfFile, CoreStore.pcaWeightsFile) ~> CoreStore.pcaProjectedFile)

  val klustaKwikClustering: Tool = unaryTool(
      "KlustaKwik Clustering", 
      klustakwikClustering,
      CoreStore.pcaProjectedFile ~> CoreStore.sampleClusterFile)
  
  val clusteringSamplesByFeatures: Tool = unaryTool(
      "Clustering Samples by Features",
      ToolKinds.clusteringSamplesByFeatures, 
      CoreStore.pcaProjectedFile ~> CoreStore.sampleClusterFile)
      
  def tools(env: LEnv): Set[Tool] = {
    env.get(Keys.genotypesId).map(checkPreExistingVcfFile(_)).toSet ++
    env.get(Keys.pcaWeightsId).map(checkPreExistingPcaWeightsFile(_)).toSet ++
    Set(extractSampleIdsFromVcfFile, importVcf, calculateSingletons, projectPcaNative, projectPca, klustaKwikClustering)
  }
  
  //TODO: TEST
  def nullaryTool(id: String, name: String, output: Store, makeRecipeSpec: LPileSpec => LRecipeSpec): Tool = {
    CoreTool(name, makeRecipeSpec(output.spec), Seq.empty, output)
  }
  
  //TODO: TEST
  def unaryTool(name: String, sig: UnarySig, makeRecipeSpec: (LPileSpec, LPileSpec) => LRecipeSpec): Tool = {
    CoreTool(
      name,
      makeRecipeSpec(sig.input.spec, sig.output.spec),
      Seq(sig.input),
      sig.output)
  }
  
  //TODO: TEST
  def unaryTool(name: String, kind: LKind, sig: UnarySig): Tool = nAryTool(name, kind, sig.toNarySig)
  
  //TODO: TEST
  def binaryTool(name: String, kind: LKind, sig: BinarySig): Tool = nAryTool(name, kind, sig.toNarySig)
  
  //TODO: TEST
  def nAryTool(name: String, kind: LKind, sig: NarySig): Tool = nAryTool(LId.LNamedId(name), kind, sig)
  
  //TODO: TEST
  def nAryTool(kind: LKind, sig: NarySig): Tool = nAryTool(LId.newAnonId, kind, sig)
  
  //TODO: TEST
  def nAryTool(id: LId, kind: LKind, sig: NarySig): Tool = {
    CoreTool(
      id,
      LRecipeSpec(kind, sig.inputs.map(_.spec), sig.output.spec),
      sig.inputs,
      sig.output)
  }
}
