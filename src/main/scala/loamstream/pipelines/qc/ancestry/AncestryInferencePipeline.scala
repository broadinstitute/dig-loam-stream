package loamstream.pipelines.qc.ancestry

import loamstream.Sigs

import loamstream.model.LPipeline
import loamstream.model.Store
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

  val genotypesStoreTool: Tool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val pcaWeightsStoreTool: Tool = CoreTool.checkPreExistingPcaWeightsFile(pcaWeightsId)
  
  val pcaProjectionTool: Tool = CoreTool.projectPca
  
  val sampleClustering: Tool = CoreTool.clusteringSamplesByFeatures
  
  val genotypesStore: Store = genotypesStoreTool.output

  val pcaWeightsStore: Store = pcaWeightsStoreTool.output
  
  val projectedValsStore: Store = pcaProjectionTool.output
  
  val sampleClustersStore: Store = sampleClustering.output

  override def stores: Set[Store] = tools.map(_.output)
  
  override val tools: Set[Tool] = Set(genotypesStoreTool, pcaWeightsStoreTool, pcaProjectionTool, sampleClustering)
}
