package loamstream.apps.minimal

import loamstream.model.calls.LPileCall
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LSig, LPile}
import loamstream.model.recipes.LRecipe

/**
 * LoamStream
 * Created by oliverr on 12/21/2015.
 */
object MiniApp extends App {

  def debugCompare(x: Any, y: Any) = {
    println("Are the following two equal: " + (x == y))
    println(x)
    println(y)
  }

  class VariantId

  class GenotypeCall

  val genotypeCallsPileId = "myGenotypecalls"
  val genotypeCallsPile =
    LPile(LSig.Map[(String, VariantId), GenotypeCall].get, PileKinds.genotypeCallsBySampleAndVariant)
  val genotypeCallsCall =
    LPileCall.apply(genotypeCallsPile, LRecipe.preExistingCheckout(genotypeCallsPileId, genotypeCallsPile))
  val sampleIdsPile = LPile(LSig.Set[Tuple1[String]].get, PileKinds.sampleIds)
  val sampleIdsCall = genotypeCallsCall.extractKey(0, sampleIdsPile, PileKinds.sampleIds)

  println(genotypeCallsCall)
  println(sampleIdsCall)
//  debugCompare(MiniMockStores.vcfFile.sig, genotypeCallsPile.sig)
  println(MiniMockStores.vcfFile.sig =:= genotypeCallsPile.sig)
  println(MiniMockTool.checkPreExistingVcfFile.recipe <:< genotypeCallsCall.recipe)
  println(LSig.Map[(String, VariantId), GenotypeCall] == LSig.Map[(String, VariantId), GenotypeCall])
  println("Yo!")

}
