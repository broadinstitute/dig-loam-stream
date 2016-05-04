package loamstream.pipelines.qc.ancestry

import loamstream.model.LPipeline
import loamstream.model.StoreSpec
import loamstream.model.ToolSpec
import loamstream.model.values.LType._
import loamstream.tools.core.CoreTool


/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
case class AncestryInferencePipeline(genotypesId: String, pcaWeightsId: String) extends LPipeline {

  val genotypesTool: ToolSpec = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val pcaWeightsTool: ToolSpec = CoreTool.checkPreExistingPcaWeightsFile(pcaWeightsId)
  
  val pcaProjectionTool: ToolSpec = CoreTool.projectPca
  
  val sampleClusteringTool: ToolSpec = CoreTool.clusteringSamplesByFeatures
  
  val genotypesStore: StoreSpec = genotypesTool.output

  val pcaWeightsStore: StoreSpec = pcaWeightsTool.output
  
  val projectedValsStore: StoreSpec = pcaProjectionTool.output
  
  val sampleClustersStore: StoreSpec = sampleClusteringTool.output

  override def stores: Set[StoreSpec] = tools.map(_.output)
  
  override val tools: Set[ToolSpec] = Set(genotypesTool, pcaWeightsTool, pcaProjectionTool, sampleClusteringTool)
}
