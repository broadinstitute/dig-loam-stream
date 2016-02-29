package loamstream.apps.minimal

import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.{LPipeline, LPipelineOps}

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object MiniPipeline {

  class VariantId

  class GenotypeCall

  val genotypeCallsPileId = "mini"
  val genotypeCallsPile =
    LPile(genotypeCallsPileId, LSig.Map[(String, VariantId), GenotypeCall].get,
      PileKinds.genotypeCallsBySampleAndVariant)
  val genotypeCallsRecipe = LRecipe.preExistingCheckout(genotypeCallsPileId, genotypeCallsPile)
  val sampleIdsPile = LPipelineOps.extractKeyPile(genotypeCallsPile, 0, PileKinds.sampleIds)
  val sampleIdsRecipe = LPipelineOps.extractKeyRecipe(genotypeCallsPile, 0, sampleIdsPile)

  val pipeline = LPipeline(genotypeCallsPile, sampleIdsPile)(genotypeCallsRecipe, sampleIdsRecipe)

}
