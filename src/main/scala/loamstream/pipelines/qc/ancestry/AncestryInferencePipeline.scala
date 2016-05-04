package loamstream.pipelines.qc.ancestry

import loamstream.model.LPipeline
import loamstream.model.StoreSpec
import loamstream.model.ToolSpec
import loamstream.model.values.LType._
import loamstream.tools.core.CoreToolSpec


/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
case class AncestryInferencePipeline(genotypesId: String, pcaWeightsId: String) extends LPipeline {

  val genotypesTool: ToolSpec = CoreToolSpec.checkPreExistingVcfFile(genotypesId)
  
  val pcaWeightsTool: ToolSpec = CoreToolSpec.checkPreExistingPcaWeightsFile(pcaWeightsId)
  
  val pcaProjectionTool: ToolSpec = CoreToolSpec.projectPca
  
  val sampleClusteringTool: ToolSpec = CoreToolSpec.clusteringSamplesByFeatures
  
  val genotypesStore: StoreSpec = genotypesTool.output

  val pcaWeightsStore: StoreSpec = pcaWeightsTool.output
  
  val projectedValsStore: StoreSpec = pcaProjectionTool.output
  
  val sampleClustersStore: StoreSpec = sampleClusteringTool.output

  override def stores: Set[StoreSpec] = tools.map(_.output)
  
  override val tools: Set[ToolSpec] = Set(genotypesTool, pcaWeightsTool, pcaProjectionTool, sampleClusteringTool)
}
