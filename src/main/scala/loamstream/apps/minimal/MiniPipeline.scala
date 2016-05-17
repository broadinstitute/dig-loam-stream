package loamstream.apps.minimal

import loamstream.model.LPipeline
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.tools.core.CoreTool

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
case class MiniPipeline(genotypesId: String) extends LPipeline {
  val genotypeCallsTool: Tool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  //TODO: Fragile
  private[minimal] def genotypeCallsStore: Store = genotypeCallsTool.outputs.head._2
  
  val sampleIdsTool: Tool = CoreTool.extractSampleIdsFromVcfFile

  //TODO: Fragile
  private[minimal] def sampleIdsStore: Store = sampleIdsTool.outputs.head._2
  
  override val tools: Set[Tool] = Set(genotypeCallsTool, sampleIdsTool)
  
  override def stores: Set[Store] = tools.flatMap(_.outputs.values)
}
