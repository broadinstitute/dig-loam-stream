package loamstream.apps

import loamstream.model.calls.{LSetCall, LMapCall}
import loamstream.model.calls.props.LProps
import loamstream.model.recipes.{LPileCalls, ExtractKey0}
import loamstream.model.tags.{LMapTag, LSetTag}

/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  class VariantId

  class GenotypeCall

  val genotypeCallsTag = LMapTag.forKeyTup2[String, VariantId, GenotypeCall]

  trait CanExtractSampleIds extends LProps

  val genotypeCalls = LMapCall.getPreexisting(genotypeCallsTag, "myGenotypes").withProps[ExtractKey0.CanExtractKey0]

  val sampleIdsTag = LSetTag.forKeyTup1[String]

  val sampleIds = LSetCall(sampleIdsTag, ExtractKey0.fromPile[String, LMapTag.Map2[String, VariantId, GenotypeCall],
    LMapCall[LMapTag.Map2[String, VariantId, GenotypeCall], LPileCalls.LCalls0,
      ExtractKey0.CanExtractKey0]](genotypeCalls))

  println(genotypeCallsTag)
  println(genotypeCalls)
  println(sampleIdsTag)

}
