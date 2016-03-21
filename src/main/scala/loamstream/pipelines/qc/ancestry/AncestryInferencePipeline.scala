package loamstream.pipelines.qc.ancestry

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.LPipeline
import loamstream.model.kinds.instances.{PileKinds, RecipeKinds}
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
object AncestryInferencePipeline {

  type SampleId = String
  type VariantId = String

  val genotypesPileId = "genotypes"
  val pcaWeightsPileId = "pcaWeights"

  val genotypesPile =
    LPile(genotypesPileId, LSig.Map[(VariantId, SampleId), Genotype].get, PileKinds.genotypeCallsByVariantAndSample)
  val pcaWeightsPile = LPile(pcaWeightsPileId, LSig.Map[(SampleId, Int), Double].get, PileKinds.pcaWeights)
  val projectedValsPile = LPile(LSig.Map[(SampleId, Int), Double].get, PileKinds.pcaProjected)

  val genotypesPileRecipe = LRecipe.preExistingCheckout(genotypesPileId, genotypesPile)
  val pcaWeightsPileRecipe = LRecipe.preExistingCheckout(pcaWeightsPileId, pcaWeightsPile)
  val pcaProjectionRecipe = LRecipe(RecipeKinds.pcaProjection, Seq(genotypesPile, pcaWeightsPile), projectedValsPile)

  val pipeline = LPipeline(genotypesPile, pcaWeightsPile, projectedValsPile)(genotypesPileRecipe,
    pcaWeightsPileRecipe, pcaProjectionRecipe)

}
