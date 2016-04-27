package loamstream.pipelines.qc.ancestry

import loamstream.Sigs

import loamstream.model.LPipeline
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.values.LType._
import loamstream.tools.core.CoreStore
import loamstream.tools.core.CoreTool
import loamstream.model.kinds.instances.ToolKinds


/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
case class AncestryInferencePipeline(genotypesId: String, pcaWeightsId: String) extends LPipeline {

  val genotypesPileRecipe: Tool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val pcaWeightsPileRecipe: Tool = CoreTool.checkPreExistingPcaWeightsFile(pcaWeightsId)
  
  val pcaProjectionRecipe: Tool = CoreTool.projectPca
  
  val sampleClustering: Tool = CoreTool.clusteringSamplesByFeatures
  
  val genotypesPile: Store = genotypesPileRecipe.output

  val pcaWeightsPile: Store = pcaWeightsPileRecipe.output
  
  val projectedValsPile: Store = pcaProjectionRecipe.output
  
  val sampleClustersPile: Store = sampleClustering.output
  

  override def stores = tools.map(_.output)
  
  override val tools = Set(genotypesPileRecipe, pcaWeightsPileRecipe, pcaProjectionRecipe, sampleClustering)
}
