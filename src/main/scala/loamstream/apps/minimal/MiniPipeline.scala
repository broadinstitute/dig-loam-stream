package loamstream.apps.minimal

import loamstream.model.LPipeline
import loamstream.model.StoreSpec
import loamstream.model.ToolSpec
import loamstream.tools.core.CoreToolSpec

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
case class MiniPipeline(genotypesId: String) extends LPipeline {
  val genotypeCallsTool: ToolSpec = CoreToolSpec.checkPreExistingVcfFile(genotypesId)
  
  val genotypeCallsStore: StoreSpec = genotypeCallsTool.output
  
  val sampleIdsTool: ToolSpec = CoreToolSpec.extractSampleIdsFromVcfFile

  val sampleIdsStore: StoreSpec = sampleIdsTool.output
  
  override val tools: Set[ToolSpec] = Set(genotypeCallsTool, sampleIdsTool)
  
  override def stores: Set[StoreSpec] = tools.map(_.output)
}
