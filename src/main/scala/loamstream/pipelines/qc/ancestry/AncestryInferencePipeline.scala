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
  @deprecated
  def genotypesStore: Store = genotypesTool.outputs.head

  //TODO: Fragile
  @deprecated
  def pcaWeightsStore: Store = pcaWeightsTool.outputs.head
  
  //TODO: Fragile
  @deprecated
  def projectedValsStore: Store = pcaProjectionTool.outputs.head
  
  //TODO: Fragile
  @deprecated
  def sampleClustersStore: Store = sampleClusteringTool.outputs.head

  override def stores: Set[Store] = tools.flatMap(_.outputs)
  
  override val tools: Set[Tool] = Set(genotypesTool, pcaWeightsTool, pcaProjectionTool, sampleClusteringTool)
}
