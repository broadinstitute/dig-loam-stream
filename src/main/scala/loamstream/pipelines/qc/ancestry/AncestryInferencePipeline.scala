package loamstream.pipelines.qc.ancestry

import loamstream.Sigs

import loamstream.model.LPipeline
import loamstream.model.StoreSpec
import loamstream.model.Tool
import loamstream.model.values.LType._
import loamstream.tools.core.CoreStore
import loamstream.tools.core.CoreTool
import loamstream.model.kinds.ToolKinds


/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
case class AncestryInferencePipeline(genotypesId: String, pcaWeightsId: String) extends LPipeline {

  val genotypesTool: Tool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val pcaWeightsTool: Tool = CoreTool.checkPreExistingPcaWeightsFile(pcaWeightsId)
  
  val pcaProjectionTool: Tool = CoreTool.projectPca
  
  val sampleClusteringTool: Tool = CoreTool.clusteringSamplesByFeatures
  
  val genotypesStore: StoreSpec = genotypesTool.output

  val pcaWeightsStore: StoreSpec = pcaWeightsTool.output
  
  val projectedValsStore: StoreSpec = pcaProjectionTool.output
  
  val sampleClustersStore: StoreSpec = sampleClusteringTool.output

  override def stores: Set[StoreSpec] = tools.map(_.output)
  
  override val tools: Set[Tool] = Set(genotypesTool, pcaWeightsTool, pcaProjectionTool, sampleClusteringTool)
}
