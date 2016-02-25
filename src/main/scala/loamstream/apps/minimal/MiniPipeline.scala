package loamstream.apps.minimal

import loamstream.model.LPipeline
import loamstream.model.calls.LPileCall
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.recipes.LRecipe

/**
 * LoamStream
 * Created by oliverr on 2/17/2016.
 */
object MiniPipeline {

  class VariantId

  class GenotypeCall

  val genotypeCallsPileId = "mini"
  val genotypeCallsPile =
    LPile(LSig.Map[(String, VariantId), GenotypeCall].get, PileKinds.genotypeCallsBySampleAndVariant)
  val genotypeCallsCall =
    LPileCall.apply(genotypeCallsPile, LRecipe.preExistingCheckout(genotypeCallsPileId, genotypeCallsPile))
  val sampleIdsPile = LPile(LSig.Set[Tuple1[String]].get, PileKinds.sampleIds).as(PileKinds.sampleIds)
  val sampleIdsCall = genotypeCallsCall.extractKey(0, sampleIdsPile, PileKinds.sampleIds)

  val pipeline = LPipeline(genotypeCallsCall, sampleIdsCall)

}
