package loamstream.pipelines.qc.ancestry

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.LPipeline
import loamstream.model.kinds.instances.{PileKinds, RecipeKinds}
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.signatures.Signatures.{ClusterId, SampleId, VariantId}

/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
case class AncestryInferencePipeline(genotypesId: String, pcaWeightsId: String) extends LPipeline {

  val genotypesPile =
    LPile(genotypesId, LSig.Map[(VariantId, SampleId), Genotype], PileKinds.genotypeCallsByVariantAndSample)
  val pcaWeightsPile = LPile(pcaWeightsId, LSig.Map[(SampleId, Int), Double], PileKinds.pcaWeights)
  val projectedValsPile = LPile(LSig.Map[(SampleId, Int), Double], PileKinds.pcaProjected)
  val sampleClustersPile = LPile(LSig.Map[SampleId, ClusterId], PileKinds.sampleClustersByAncestry)

  val genotypesPileRecipe = LRecipe.preExistingCheckout(genotypesId, genotypesPile)
  val pcaWeightsPileRecipe = LRecipe.preExistingCheckout(pcaWeightsId, pcaWeightsPile)
  val pcaProjectionRecipe = LRecipe(RecipeKinds.pcaProjection, Seq(genotypesPile, pcaWeightsPile), projectedValsPile)
  val sampleClustering = LRecipe(RecipeKinds.clusteringSamplesByFeatures, Seq(projectedValsPile), sampleClustersPile)

  val piles = Set(genotypesPile, pcaWeightsPile, projectedValsPile, sampleClustersPile)
  val recipes = Set(genotypesPileRecipe, pcaWeightsPileRecipe, pcaProjectionRecipe, sampleClustering)

}
