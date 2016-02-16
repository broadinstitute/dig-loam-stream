package loamstream.apps

import loamstream.model.calls.{LPileCall, LSig}
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.LPile
import loamstream.model.recipes.LCheckoutPreexisting

/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  class VariantId

  class GenotypeCall

  val genotypeCallsPile =
    LPile(LSig.Map.apply[(String, VariantId), GenotypeCall].get, PileKinds.genotypeCallsBySampleAndVariant)
  val genotypeCallsCall =
    LPileCall.apply(genotypeCallsPile, LCheckoutPreexisting("myGenotypecalls", genotypeCallsPile))
  val sampleIds = genotypeCallsCall.extractKey(0, PileKinds.sampleIds)

  println(genotypeCallsCall)
  println(sampleIds)
  println("Yo!")

}
