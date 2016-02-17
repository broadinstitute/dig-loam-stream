package loamstream.apps.minimal

import loamstream.model.piles.LSig

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

  println(MiniPipeline.genotypeCallsCall)
  println(MiniPipeline.sampleIdsCall)
  //  debugCompare(MiniMockStores.vcfFile.sig, genotypeCallsPile.sig)
  println(MiniMockStores.vcfFile.sig =:= MiniPipeline.genotypeCallsPile.sig)
  println(MiniMockTool.checkPreExistingVcfFile.recipe <:< MiniPipeline.genotypeCallsCall.recipe)
  println(LSig.Map[(String, MiniPipeline.VariantId), MiniPipeline.GenotypeCall] ==
    LSig.Map[(String, MiniPipeline.VariantId), MiniPipeline.GenotypeCall])
  println("Yo!")

}
