package loamstream.apps

import loamstream.model.calls.LMapCall
import loamstream.model.recipes.{LCheckoutPreexisting, LPileCalls}
import loamstream.model.tags.LSemTag

/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  class VariantId

  class GenotypeCall

  //  val genotypeCallsTag = LMapTag.forKeyTup2[String, VariantId, GenotypeCall]

  //  val genotypeCalls = LMapCall.getPreexisting(genotypeCallsTag, "myGenotypes")

  val genotypeCalls =
    LMapCall.apply[LMapCall.Map2[String, VariantId, GenotypeCall], LSemTag,
      LPileCalls.LCalls0](LCheckoutPreexisting("myGenotypecalls"))

  //  val sampleIds = ExtractKey0Call(genotypeCalls)

  //  val sampleIdsTag = LSetTag.forKeyTup1[String]

  //  val sampleIds = LSetCall(sampleIdsTag, ExtractKey0.fromPile[String, LMapTag.Map2[String, VariantId, GenotypeCall],
  //    LMapCall[LMapTag.Map2[String, VariantId, GenotypeCall], LPileCalls.LCalls0,
  //      ExtractKey0.CanExtractKey0]](genotypeCalls))

  //  println(genotypeCallsTag)
  //  println(genotypeCalls)
  //  println(sampleIdsTag)

  println(genotypeCalls)
  println("Yo!")

}
