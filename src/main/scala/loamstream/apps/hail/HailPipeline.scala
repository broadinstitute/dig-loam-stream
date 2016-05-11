package loamstream.apps.hail

import loamstream.Sigs
import loamstream.model.{LPipeline, LPipelineOps}
import loamstream.model.Store
import loamstream.model.Tool

import loamstream.tools.core.CoreStore
import loamstream.model.kinds.StoreKinds
import loamstream.tools.core.CoreTool

/**
  * Created on: 3/10/2016
  *
  * @author Kaan Yuksel
  */
case class HailPipeline(genotypesId: String, vdsId: String, singletonsId: String) extends LPipeline {
  val genotypeCallsTool: Tool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val vdsTool: Tool = CoreTool.importVcf
  
  val singletonTool: Tool = CoreTool.calculateSingletons

  override def stores: Set[Store] = tools.flatMap(_.outputs)
  
  override val tools: Set[Tool] = Set(genotypeCallsTool, vdsTool, singletonTool)
}
