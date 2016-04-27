package loamstream.pipelines.qc.ancestry

import loamstream.model.LPipeline
import loamstream.model.kinds.instances.{PileKinds, RecipeKinds}
import loamstream.model.LSig
import loamstream.model.recipes.LRecipe
import loamstream.model.values.LType._
import loamstream.model.Store
import loamstream.tools.core.CoreStore
import loamstream.Sigs


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

  val genotypesPileRecipe: LRecipe = LRecipe.preExistingCheckout(genotypesId, genotypesPile)
  
  val pcaWeightsPileRecipe: LRecipe = LRecipe.preExistingCheckout(pcaWeightsId, pcaWeightsPile)
  
  val pcaProjectionRecipe: LRecipe = LRecipe(RecipeKinds.pcaProjection, Seq(genotypesPile, pcaWeightsPile), projectedValsPile)
  
  val sampleClustering: LRecipe = LRecipe(RecipeKinds.clusteringSamplesByFeatures, Seq(projectedValsPile), sampleClustersPile)

  override val piles = Set(genotypesPile, pcaWeightsPile, projectedValsPile, sampleClustersPile)
  
  override val recipes = Set(genotypesPileRecipe, pcaWeightsPileRecipe, pcaProjectionRecipe, sampleClustering)
}
