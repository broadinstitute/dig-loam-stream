package loamstream.apps.hail

import loamstream.Sigs
import loamstream.model.{LPipeline, LPipelineOps}
import loamstream.model.StoreSpec
import loamstream.model.ToolSpec

import loamstream.tools.core.CoreStoreSpec
import loamstream.model.kinds.StoreKinds
import loamstream.tools.core.CoreToolSpec

/**
  * Created on: 3/10/2016
  *
  * @author Kaan Yuksel
  */
case class HailPipeline(genotypesId: String, vdsId: String, singletonsId: String) extends LPipeline {
  val genotypeCallsTool: ToolSpec = CoreToolSpec.checkPreExistingVcfFile(genotypesId)
  
  val vdsTool: ToolSpec = CoreToolSpec.importVcf
  
  val singletonTool: ToolSpec = CoreToolSpec.calculateSingletons

  override def stores: Set[StoreSpec] = tools.map(_.output)
  
  override val tools: Set[ToolSpec] = Set(genotypeCallsTool, vdsTool, singletonTool)
}
