package loamstream.apps.minimal

import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.LSig
import loamstream.model.recipes.LRecipe
import loamstream.model.values.LType.LTuple.LTuple2
import loamstream.model.values.LType.{LGenotype, LSampleId, LVariantId}
import loamstream.model.{LPipeline, LPipelineOps}
import loamstream.model.Store
import loamstream.tools.core.CoreStore
import loamstream.Sigs

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
case class MiniPipeline(genotypesId: String) extends LPipeline {
  val genotypeCallsPile: Store = CoreStore(
      genotypesId, 
      Sigs.variantAndSampleToGenotype, 
      PileKinds.genotypeCallsByVariantAndSample)
      
  val genotypeCallsRecipe: LRecipe = LRecipe.preExistingCheckout(genotypesId, genotypeCallsPile)
  
  val sampleIdsPile: Store = {
    LPipelineOps.extractKeyPile(genotypeCallsPile, PileKinds.sampleKeyIndexInGenotypes, PileKinds.sampleIds)
  }
  
  val sampleIdsRecipe: LRecipe = {
    LPipelineOps.extractKeyRecipe(genotypeCallsPile, PileKinds.sampleKeyIndexInGenotypes, sampleIdsPile)
  }

  override val piles = Set(genotypeCallsPile, sampleIdsPile)
  override val recipes = Set(genotypeCallsRecipe, sampleIdsRecipe)
}
