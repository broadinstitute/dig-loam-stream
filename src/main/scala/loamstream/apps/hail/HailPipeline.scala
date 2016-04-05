package loamstream.apps.hail

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.signatures.Signatures.{SingletonCount, VariantId}
import loamstream.model.{LPipeline, LPipelineOps}

/**
  * Created on: 3/10/2016
  *
  * @author Kaan Yuksel
  */
case class HailPipeline(genotypesId: String, vdsId: String, singletonsId: String) extends LPipeline {
  val genotypeCallsPile = LPile(genotypesId, LSig.Map[(String, VariantId), Genotype],
    PileKinds.genotypeCallsByVariantAndSample)
  val genotypeCallsRecipe = LRecipe.preExistingCheckout(genotypesId, genotypeCallsPile)
  val vdsPile = LPile(vdsId, LSig.Map[(String, VariantId), Genotype], PileKinds.genotypeCallsByVariantAndSample)
  val vdsRecipe = LPipelineOps.importVcfRecipe(genotypeCallsPile, 0, vdsPile)
  val singletonPile = LPile(singletonsId, LSig.Map[Tuple1[String], SingletonCount], PileKinds.singletonCounts)
  val singletonRecipe = LPipelineOps.calculateSingletonsRecipe(vdsPile, 0, singletonPile)

  val piles = Set(genotypeCallsPile, vdsPile, singletonPile)
  val recipes = Set(genotypeCallsRecipe, vdsRecipe, singletonRecipe)

}
