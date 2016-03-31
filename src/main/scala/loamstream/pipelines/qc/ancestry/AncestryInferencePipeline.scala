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
object AncestryInferencePipeline(gneotypesId:String, pcaWeightsId:String) {

  val genotypesPileId = "genotypes"
  val pcaWeightsPileId = "pcaWeights"

  val genotypesPile =
    LPile(genotypesPileId, LSig.Map[(VariantId, SampleId), Genotype], PileKinds.genotypeCallsByVariantAndSample)
  val pcaWeightsPile = LPile(pcaWeightsPileId, LSig.Map[(SampleId, Int), Double], PileKinds.pcaWeights)
  val projectedValsPile = LPile(LSig.Map[(SampleId, Int), Double], PileKinds.pcaProjected)
  val sampleClustersPile = LPile(LSig.Map[SampleId, ClusterId], PileKinds.sampleClustersByAncestry)

  val genotypesPileRecipe = LRecipe.preExistingCheckout(genotypesPileId, genotypesPile)
  val pcaWeightsPileRecipe = LRecipe.preExistingCheckout(pcaWeightsPileId, pcaWeightsPile)
  val pcaProjectionRecipe = LRecipe(RecipeKinds.pcaProjection, Seq(genotypesPile, pcaWeightsPile), projectedValsPile)
  val sampleClustering = LRecipe(RecipeKinds.clusteringSamplesByFeatures, Seq(projectedValsPile), sampleClustersPile)

  val pipeline =
    LPipeline(genotypesPile, pcaWeightsPile, projectedValsPile, sampleClustersPile)(genotypesPileRecipe,
      pcaWeightsPileRecipe, pcaProjectionRecipe, sampleClustering)

}
