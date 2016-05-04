package loamstream.tools.core

import loamstream.LEnv
import loamstream.model.LId
import loamstream.model.LId.LNamedId
import loamstream.model.kinds.ToolKinds.{klustakwikClustering, nativePcaProjection, pcaProjection}
import loamstream.model.ToolSpec
import LCoreEnv._
import loamstream.model.StoreSpec
import loamstream.model.kinds.LKind
import loamstream.model.kinds.ToolKinds
import loamstream.model.kinds.StoreKinds

/**
  * LoamStream
  * @author oliverr 
  * @date 2/16/2016.
  * @author Clint
  */
object CoreTool {
  
  import StoreOps._
  
  def checkPreExistingVcfFile(id: String): ToolSpec = ToolSpec.preExistingCheckout(id)(CoreStore.vcfFile)

  def checkPreExistingPcaWeightsFile(id: String): ToolSpec = ToolSpec.preExistingCheckout(id)(CoreStore.pcaWeightsFile)

  val extractSampleIdsFromVcfFile: ToolSpec = {
    (CoreStore.vcfFile ~> CoreStore.sampleIdsFile).as {
      ToolSpec.keyExtraction(StoreKinds.sampleKeyIndexInGenotypes)
    }
  }

  val importVcf: ToolSpec = {
    (CoreStore.vcfFile ~> CoreStore.vdsFile).as(ToolSpec.vcfImport(0))
  }

  val calculateSingletons: ToolSpec = {
    (CoreStore.vdsFile ~> CoreStore.singletonsFile).as(ToolSpec.calculateSingletons(0))
  }

  val projectPcaNative: ToolSpec = binaryTool(
      nativePcaProjection,
      (CoreStore.vcfFile, CoreStore.pcaWeightsFile) ~> CoreStore.pcaProjectedFile)
      
  val projectPca: ToolSpec = binaryTool(
      pcaProjection,
       (CoreStore.vcfFile, CoreStore.pcaWeightsFile) ~> CoreStore.pcaProjectedFile)

  val klustaKwikClustering: ToolSpec = unaryTool(
      klustakwikClustering,
      CoreStore.pcaProjectedFile ~> CoreStore.sampleClusterFile)
  
  val clusteringSamplesByFeatures: ToolSpec = unaryTool(
      ToolKinds.clusteringSamplesByFeatures, 
      CoreStore.pcaProjectedFile ~> CoreStore.sampleClusterFile)
      
  def tools(env: LEnv): Set[ToolSpec] = {
    env.get(Keys.genotypesId).map(checkPreExistingVcfFile(_)).toSet ++
    env.get(Keys.pcaWeightsId).map(checkPreExistingPcaWeightsFile(_)).toSet ++
    Set(extractSampleIdsFromVcfFile, importVcf, calculateSingletons, projectPcaNative, projectPca, klustaKwikClustering)
  }
  
  //TODO: TEST
  def unaryTool(kind: LKind, sig: UnarySig): ToolSpec = nAryTool(kind, sig.toNarySig)
  
  //TODO: TEST
  def binaryTool(kind: LKind, sig: BinarySig): ToolSpec = nAryTool(kind, sig.toNarySig)
  
  //TODO: TEST
  def nAryTool(kind: LKind, sig: NarySig): ToolSpec = ToolSpec(kind, sig.inputs, sig.output)
}
