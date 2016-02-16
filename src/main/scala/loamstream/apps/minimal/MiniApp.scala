package loamstream.apps.minimal

import loamstream.model.calls.{LPileCall, LSig}
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe

/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  class VariantId

  class GenotypeCall

  val genotypeCallsPileId = "myGenotypecalls"
  val genotypeCallsPile =
    LPile(LSig.Map.apply[(String, VariantId), GenotypeCall].get, PileKinds.genotypeCallsBySampleAndVariant)
  val genotypeCallsCall =
    LPileCall.apply(genotypeCallsPile, LRecipe.preExistingCheckout(genotypeCallsPileId, genotypeCallsPile))
  val sampleIdsPile = LPile(LSig.Set.apply[Tuple1[String]].get, PileKinds.sampleIds)
  val sampleIdsCall = genotypeCallsCall.extractKey(0, sampleIdsPile, PileKinds.sampleIds)

  println(genotypeCallsCall)
  println(sampleIdsCall)
  println(MiniMockStores.vcfFile.sig == genotypeCallsPile.sig)
  println(MiniMockTool.checkPreExistingVcfFile.recipe <:< genotypeCallsCall.recipe)
  println("Yo!")

}
