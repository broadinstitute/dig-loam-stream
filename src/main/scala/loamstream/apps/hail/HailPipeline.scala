package loamstream.apps.hail

import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.signatures.Signatures.{GenotypeCall, SingletonCount, VariantId}
import loamstream.model.{LPipeline, LPipelineOps}

/**
 * Created on: 3/10/2016 
 * @author Kaan Yuksel 
 */
object HailPipeline {
  val genotypeCallsPileId = "genotypes"
  val vdsPileId = "variant dataset"
  val singletonPileId = "singleton counts"
  val genotypeCallsPile = LPile(genotypeCallsPileId, LSig.Map[(String, VariantId), GenotypeCall].get,
      PileKinds.genotypeCallsBySampleAndVariant)
  val genotypeCallsRecipe = LRecipe.preExistingCheckout(genotypeCallsPileId, genotypeCallsPile)
  val vdsPile = LPile(vdsPileId, LSig.Map[(String, VariantId), GenotypeCall].get,
    PileKinds.genotypeCallsBySampleAndVariant)
  val vdsRecipe = LPipelineOps.importVcfRecipe(genotypeCallsPile, 0, vdsPile)
  val singletonPile = LPile(singletonPileId, LSig.Map[Tuple1[String], SingletonCount].get,
    PileKinds.singletonCounts)
  val singletonRecipe = LPipelineOps.calculateSingletonsRecipe(vdsPile, 0, singletonPile)

  val pipeline = LPipeline(genotypeCallsPile, vdsPile, singletonPile)(genotypeCallsRecipe, vdsRecipe, singletonRecipe)


}
