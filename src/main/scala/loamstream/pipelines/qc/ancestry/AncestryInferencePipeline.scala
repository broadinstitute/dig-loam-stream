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

  val genotypesTool: Tool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val pcaWeightsTool: Tool = CoreTool.checkPreExistingPcaWeightsFile(pcaWeightsId)
  
  val pcaProjectionTool: Tool = CoreTool.projectPca
  
  val sampleClusteringTool: Tool = CoreTool.clusteringSamplesByFeatures
  
  //TODO: Fragile
  private[ancestry] def genotypesStore: Store = genotypesTool.outputs.head._2

  //TODO: Fragile
  private[ancestry] def pcaWeightsStore: Store = pcaWeightsTool.outputs.head._2
  
  //TODO: Fragile
  private[ancestry] def projectedValsStore: Store = pcaProjectionTool.outputs.head._2
  
  //TODO: Fragile
  private[ancestry] def sampleClustersStore: Store = sampleClusteringTool.outputs.head._2

  override def stores: Set[Store] = tools.flatMap(_.outputs.values)
  
  override val tools: Set[Tool] = Set(genotypesTool, pcaWeightsTool, pcaProjectionTool, sampleClusteringTool)
}
