package loamstream.apps

import loamstream.model.calls.LMapPreexisting
import loamstream.model.tags.{LMapTag, LSetTag}

/**
  * LoamStream
  * Created by oliverr on 12/21/2015.
  */
object MiniApp extends App {

  class VariantId

  class GenotypeCall

  val genotypeCallsTag = LMapTag.forKeyTup2[String, VariantId, GenotypeCall]

  val genotypeCalls = LMapPreexisting(genotypeCallsTag, "myGenotypes")

  val sampleIdsTag = LSetTag.forKeyTup1[String]

  println(genotypeCallsTag)
  println(genotypeCalls)
  println(sampleIdsTag)

}
