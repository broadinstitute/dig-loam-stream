package loamstream.pipelines.qc.ancestry

import loamstream.Sigs

import loamstream.model.LPipeline
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.kinds.instances.{PileKinds, RecipeKinds}
import loamstream.model.values.LType._
import loamstream.tools.core.CoreStore


/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
case class AncestryInferencePipeline(genotypesId: String, pcaWeightsId: String) extends LPipeline {

  val genotypesPile: Store = CoreStore(
      genotypesId, 
      Sigs.variantAndSampleToGenotype, 
      PileKinds.genotypeCallsByVariantAndSample)

  val pcaWeightsPile: Store = CoreStore(
      pcaWeightsId, 
      Sigs.sampleIdAndIntToDouble, 
      PileKinds.pcaWeights)
  
  val projectedValsPile: Store = CoreStore(Sigs.sampleIdAndIntToDouble, PileKinds.pcaProjected)
  
  val sampleClustersPile: Store = CoreStore(LSampleId to LClusterId, PileKinds.sampleClustersByAncestry)

  val genotypesPileRecipe: Tool = Tool.preExistingCheckout(genotypesId, genotypesPile)
  
  val pcaWeightsPileRecipe: Tool = Tool.preExistingCheckout(pcaWeightsId, pcaWeightsPile)
  
  val pcaProjectionRecipe: Tool = Tool(RecipeKinds.pcaProjection, Seq(genotypesPile, pcaWeightsPile), projectedValsPile)
  
  val sampleClustering: Tool = Tool(RecipeKinds.clusteringSamplesByFeatures, Seq(projectedValsPile), sampleClustersPile)

  override val piles = Set(genotypesPile, pcaWeightsPile, projectedValsPile, sampleClustersPile)
  
  override val recipes = Set(genotypesPileRecipe, pcaWeightsPileRecipe, pcaProjectionRecipe, sampleClustering)
}
