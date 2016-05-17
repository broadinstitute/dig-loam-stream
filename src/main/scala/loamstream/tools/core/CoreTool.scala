package loamstream.tools.core

import loamstream.LEnv
import loamstream.model.LId
import loamstream.model.LId.LNamedId
import loamstream.model.kinds.ToolKinds.{klustakwikClustering, nativePcaProjection, pcaProjection}
import loamstream.model.ToolSpec
import LCoreEnv._
import loamstream.model.Tool
import loamstream.model.Store
import loamstream.model.StoreSpec
import loamstream.model.kinds.LKind
import loamstream.model.kinds.ToolKinds
import loamstream.model.kinds.StoreKinds

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
final case class CoreTool(id: LId, spec: ToolSpec, inputs: Map[LId, Store], outputs: Map[LId, Store]) extends Tool

object CoreTool {
  
  def apply(name: String, toolSpec: ToolSpec, inputs: Map[LId, Store], outputs: Map[LId, Store]): CoreTool = {
    CoreTool(LNamedId(name), toolSpec, inputs, outputs)
  }
  
  import StoreOps._
  
  def checkPreExistingVcfFile(id: String): Tool = nullaryTool(
      id, 
      "What a nice VCF file!",
      CoreStore.vcfFile,
      ToolSpec.preExistingCheckout(id))

  def checkPreExistingPcaWeightsFile(id: String): Tool = nullaryTool(
      id, 
      "File with PCA weights",
      CoreStore.pcaWeightsFile,
      ToolSpec.preExistingCheckout(id))

  val extractSampleIdsFromVcfFile: Tool = unaryTool(
      "Extracted sample ids from VCF file into a text file.",
      CoreStore.vcfFile ~> CoreStore.sampleIdsFile,
      ToolSpec.keyExtraction(StoreKinds.sampleKeyIndexInGenotypes) _)

  val importVcf: Tool = unaryTool(
      "Import VCF file into VDS format Hail works with.",
      CoreStore.vcfFile ~> CoreStore.vdsFile,
      ToolSpec.vcfImport(0) _)

  val calculateSingletons: Tool = unaryTool(
      "Calculate singletons from genotype calls in VDS format.",
      CoreStore.vdsFile ~> CoreStore.singletonsFile,
      ToolSpec.calculateSingletons(0) _)

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
  def nullaryTool(id: String, name: String, output: Store, makeToolSpec: StoreSpec => ToolSpec): Tool = {
    CoreTool(
        name, 
        makeToolSpec(output.spec), 
        Map.empty[LId, Store], 
        Map(output.id -> output)) //TODO: correct?
  }
  
  //TODO: TEST
  def unaryTool(name: String, sig: UnarySig, makeToolSpec: (StoreSpec, StoreSpec) => ToolSpec): Tool = {
    CoreTool(
      name,
      makeToolSpec(sig.input.spec, sig.output.spec),
      Map(sig.input.id -> sig.input), //TODO: correct?
      Map(sig.output.id -> sig.output)) //TODO: correct?
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
      ToolSpec(kind, sig.inputs.map(_.toTuple).toMap, sig.outputs.map(_.toTuple).toMap),
      sig.inputs.map(i => i.id -> i).toMap,  //TODO: correct?
      sig.outputs.map(o => o.id -> o).toMap) //TODO: correct?
  }
}
