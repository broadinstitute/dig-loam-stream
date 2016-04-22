package loamstream.apps.hail

import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe
import loamstream.model.values.LType.LTuple.{LTuple1, LTuple2}
import loamstream.model.values.LType.{LGenotype, LSampleId, LSingletonCount, LString, LVariantId}
import loamstream.model.{LPipeline, LPipelineOps}

/**
  * Created on: 3/10/2016
  *
  * @author Kaan Yuksel
  */
case class HailPipeline(genotypesId: String, vdsId: String, singletonsId: String) extends LPipeline {
  val genotypeCallsPile = LPile(genotypesId, LSig.Map(LTuple2(LVariantId, LSampleId), LGenotype),
    PileKinds.genotypeCallsByVariantAndSample)
  val genotypeCallsRecipe = LRecipe.preExistingCheckout(genotypesId, genotypeCallsPile)
  val vdsPile =
    LPile(vdsId, LSig.Map(LTuple2(LVariantId, LSampleId), LGenotype), PileKinds.genotypeCallsByVariantAndSample)
  val vdsRecipe = LPipelineOps.importVcfRecipe(genotypeCallsPile, 0, vdsPile)
  val singletonPile = LPile(singletonsId, LSig.Map(LTuple1(LSampleId), LSingletonCount), PileKinds.singletonCounts)
  val singletonRecipe = LPipelineOps.calculateSingletonsRecipe(vdsPile, 0, singletonPile)

  val piles = Set(genotypeCallsPile, vdsPile, singletonPile)
  val recipes = Set(genotypeCallsRecipe, vdsRecipe, singletonRecipe)

}
