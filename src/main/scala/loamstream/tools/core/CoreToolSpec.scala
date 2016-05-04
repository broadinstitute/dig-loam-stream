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
object CoreToolSpec {
  
  import StoreOps._
  
  def checkPreExistingVcfFile(id: String): ToolSpec = ToolSpec.preExistingCheckout(id)(CoreStoreSpec.vcfFile)

  def checkPreExistingPcaWeightsFile(id: String): ToolSpec = ToolSpec.preExistingCheckout(id)(CoreStoreSpec.pcaWeightsFile)

  val extractSampleIdsFromVcfFile: ToolSpec = {
    (CoreStoreSpec.vcfFile ~> CoreStoreSpec.sampleIdsFile).as {
      ToolSpec.keyExtraction(StoreKinds.sampleKeyIndexInGenotypes)
    }
  }

  val importVcf: ToolSpec = {
    (CoreStoreSpec.vcfFile ~> CoreStoreSpec.vdsFile).as(ToolSpec.vcfImport(0))
  }

  val calculateSingletons: ToolSpec = {
    (CoreStoreSpec.vdsFile ~> CoreStoreSpec.singletonsFile).as(ToolSpec.calculateSingletons(0))
  }

  val projectPcaNative: ToolSpec = binaryTool(
      nativePcaProjection,
      (CoreStoreSpec.vcfFile, CoreStoreSpec.pcaWeightsFile) ~> CoreStoreSpec.pcaProjectedFile)
      
  val projectPca: ToolSpec = binaryTool(
      pcaProjection,
       (CoreStoreSpec.vcfFile, CoreStoreSpec.pcaWeightsFile) ~> CoreStoreSpec.pcaProjectedFile)

  val klustaKwikClustering: ToolSpec = unaryTool(
      klustakwikClustering,
      CoreStoreSpec.pcaProjectedFile ~> CoreStoreSpec.sampleClusterFile)
  
  val clusteringSamplesByFeatures: ToolSpec = unaryTool(
      ToolKinds.clusteringSamplesByFeatures, 
      CoreStoreSpec.pcaProjectedFile ~> CoreStoreSpec.sampleClusterFile)
      
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
