package loamstream.apps.hail

import loamstream.Sigs
import loamstream.model.{LPipeline, LPipelineOps}
import loamstream.model.Store
import loamstream.model.ToolBase

import loamstream.model.kinds.instances.PileKinds
import loamstream.tools.core.CoreStore

/**
  * Created on: 3/10/2016
  *
  * @author Kaan Yuksel
  */
case class HailPipeline(genotypesId: String, vdsId: String, singletonsId: String) extends LPipeline {
  import Sigs._
  
  val genotypeCallsPile: Store = CoreStore(
      genotypesId, 
      Sigs.variantAndSampleToGenotype,
      PileKinds.genotypeCallsByVariantAndSample)
    
  val genotypeCallsRecipe: ToolBase = ToolBase.preExistingCheckout(genotypesId, genotypeCallsPile)
  
  val vdsPile: Store = CoreStore(
      vdsId, 
      Sigs.variantAndSampleToGenotype, 
      PileKinds.genotypeCallsByVariantAndSample)
  
  val vdsRecipe: ToolBase = LPipelineOps.importVcfRecipe(genotypeCallsPile, 0, vdsPile)
  
  val singletonPile: Store = CoreStore(singletonsId, Sigs.sampleToSingletonCount, PileKinds.singletonCounts)
  
  val singletonRecipe: ToolBase = LPipelineOps.calculateSingletonsRecipe(vdsPile, 0, singletonPile)

  override val piles: Set[Store] = Set(genotypeCallsPile, vdsPile, singletonPile)
  
  override val recipes: Set[ToolBase] = Set(genotypeCallsRecipe, vdsRecipe, singletonRecipe)
}
