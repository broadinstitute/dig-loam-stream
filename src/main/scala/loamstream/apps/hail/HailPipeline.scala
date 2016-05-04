package loamstream.apps.hail

import loamstream.Sigs
import loamstream.model.{LPipeline, LPipelineOps}
import loamstream.model.StoreSpec
import loamstream.model.ToolSpec

import loamstream.tools.core.CoreStore
import loamstream.model.kinds.StoreKinds
import loamstream.tools.core.CoreTool

/**
  * Created on: 3/10/2016
  *
  * @author Kaan Yuksel
  */
case class HailPipeline(genotypesId: String, vdsId: String, singletonsId: String) extends LPipeline {
  val genotypeCallsTool: ToolSpec = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val vdsTool: ToolSpec = CoreTool.importVcf
  
  val singletonTool: ToolSpec = CoreTool.calculateSingletons

  override def stores: Set[StoreSpec] = tools.map(_.output)
  
  override val tools: Set[ToolSpec] = Set(genotypeCallsTool, vdsTool, singletonTool)
}
