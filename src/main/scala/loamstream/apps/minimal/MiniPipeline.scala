package loamstream.apps.minimal

import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.{LPipeline, LPipelineOps}
import loamstream.model.signatures.Signatures.VariantId
import loamstream.model.signatures.Signatures.GenotypeCall

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object MiniPipeline {
  val genotypeCallsPileId = "genotypes"
  val genotypeCallsPile = LPile(genotypeCallsPileId, LSig.Map[(String, VariantId), GenotypeCall].get,
      PileKinds.genotypeCallsBySampleAndVariant)
  val genotypeCallsRecipe = LRecipe.preExistingCheckout(genotypeCallsPileId, genotypeCallsPile)
  val sampleIdsPile = LPipelineOps.extractKeyPile(genotypeCallsPile, 0, PileKinds.sampleIds)
  val sampleIdsRecipe = LPipelineOps.extractKeyRecipe(genotypeCallsPile, 0, sampleIdsPile)

  val pipeline = LPipeline(genotypeCallsPile, sampleIdsPile)(genotypeCallsRecipe, sampleIdsRecipe)
}
