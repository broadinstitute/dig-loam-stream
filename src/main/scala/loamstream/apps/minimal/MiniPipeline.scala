package loamstream.apps.minimal

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.signatures.Signatures.{SampleId, VariantId}
import loamstream.model.{LPipeline, LPipelineOps}

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
object MiniPipeline {
  val genotypeCallsPileId = "genotypes"
  val genotypeCallsPile =
    LPile(genotypeCallsPileId, LSig.Map[(VariantId, SampleId), Genotype], PileKinds.genotypeCallsByVariantAndSample)
  val genotypeCallsRecipe = LRecipe.preExistingCheckout(genotypeCallsPileId, genotypeCallsPile)
  val sampleIdsPile =
    LPipelineOps.extractKeyPile(genotypeCallsPile, PileKinds.sampleKeyIndexInGenotypes, PileKinds.sampleIds)
  val sampleIdsRecipe = LPipelineOps.extractKeyRecipe(genotypeCallsPile, PileKinds.sampleKeyIndexInGenotypes,
    sampleIdsPile)

  val pipeline = LPipeline(genotypeCallsPile, sampleIdsPile)(genotypeCallsRecipe, sampleIdsRecipe)
}
