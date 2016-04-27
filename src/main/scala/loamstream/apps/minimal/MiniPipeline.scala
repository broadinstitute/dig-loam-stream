package loamstream.apps.minimal

import loamstream.Sigs
import loamstream.model.{LPipeline, LPipelineOps}
import loamstream.model.Store
import loamstream.model.ToolBase

import loamstream.model.kinds.instances.PileKinds
import loamstream.tools.core.CoreStore

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
case class MiniPipeline(genotypesId: String) extends LPipeline {
  val genotypeCallsPile: Store = CoreStore(
      genotypesId, 
      Sigs.variantAndSampleToGenotype, 
      PileKinds.genotypeCallsByVariantAndSample)
      
  val genotypeCallsRecipe: ToolBase = ToolBase.preExistingCheckout(genotypesId, genotypeCallsPile)
  
  val sampleIdsPile: Store = {
    LPipelineOps.extractKeyPile(genotypeCallsPile, PileKinds.sampleKeyIndexInGenotypes, PileKinds.sampleIds)
  }
  
  val sampleIdsRecipe: ToolBase = {
    LPipelineOps.extractKeyRecipe(genotypeCallsPile, PileKinds.sampleKeyIndexInGenotypes, sampleIdsPile)
  }

  override val piles = Set(genotypeCallsPile, sampleIdsPile)
  override val recipes = Set(genotypeCallsRecipe, sampleIdsRecipe)
}
