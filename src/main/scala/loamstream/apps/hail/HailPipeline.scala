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
  val genotypeCallsRecipe: Tool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  val vdsRecipe: Tool = CoreTool.importVcf
  
  val singletonRecipe: Tool = CoreTool.calculateSingletons

  override def stores: Set[Store] = tools.map(_.output)
  
  override val tools: Set[Tool] = Set(genotypeCallsRecipe, vdsRecipe, singletonRecipe)
}
