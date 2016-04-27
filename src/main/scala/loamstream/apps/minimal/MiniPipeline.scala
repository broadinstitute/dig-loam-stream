package loamstream.apps.minimal

import loamstream.Sigs
import loamstream.model.{LPipeline, LPipelineOps}
import loamstream.model.Store
import loamstream.model.Tool

import loamstream.model.kinds.instances.PileKinds
import loamstream.tools.core.CoreStore
import loamstream.tools.core.CoreTool

/**
  * LoamStream
  * Created by oliverr on 2/17/2016.
  */
case class MiniPipeline(genotypesId: String) extends LPipeline {
  val genotypeCallsTool: Tool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val genotypeCallsStore: Store = genotypeCallsTool.output
  
  val sampleIdsTool: Tool = CoreTool.extractSampleIdsFromVcfFile

  val sampleIdsStore: Store = sampleIdsTool.output
  
  override val tools = Set(genotypeCallsTool, sampleIdsTool)
  
  override def stores = tools.map(_.output)
}
