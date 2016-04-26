package loamstream.apps.hail

import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.LSig
import loamstream.model.recipes.LRecipe
import loamstream.model.{LPipeline, LPipelineOps}
import loamstream.model.StoreBase
import loamstream.Sigs
import loamstream.tools.core.CoreStore

/**
  * Created on: 3/10/2016
  *
  * @author Kaan Yuksel
  */
case class HailPipeline(genotypesId: String, vdsId: String, singletonsId: String) extends LPipeline {
  import Sigs._
  
  val genotypeCallsPile: StoreBase = CoreStore(
      genotypesId, 
      Sigs.variantAndSampleToGenotype,
      PileKinds.genotypeCallsByVariantAndSample)
    
  val genotypeCallsRecipe: LRecipe = LRecipe.preExistingCheckout(genotypesId, genotypeCallsPile)
  
  val vdsPile: StoreBase = CoreStore(
      vdsId, 
      Sigs.variantAndSampleToGenotype, 
      PileKinds.genotypeCallsByVariantAndSample)
  
  val vdsRecipe: LRecipe = LPipelineOps.importVcfRecipe(genotypeCallsPile, 0, vdsPile)
  
  val singletonPile: StoreBase = CoreStore(singletonsId, Sigs.sampleToSingletonCount, PileKinds.singletonCounts)
  
  val singletonRecipe: LRecipe = LPipelineOps.calculateSingletonsRecipe(vdsPile, 0, singletonPile)

  override val piles: Set[StoreBase] = Set(genotypeCallsPile, vdsPile, singletonPile)
  
  override val recipes: Set[LRecipe] = Set(genotypeCallsRecipe, vdsRecipe, singletonRecipe)
}
