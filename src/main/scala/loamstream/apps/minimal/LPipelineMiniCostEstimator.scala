package loamstream.apps.minimal

import loamstream.cost.LPipelineCostEstimator
import loamstream.map.LToolMapping
import loamstream.tools.core.CoreStore
import loamstream.tools.core.CoreTool
import loamstream.model.Store
import loamstream.model.ToolBase

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
case class LPipelineMiniCostEstimator(genotypesId: String) extends LPipelineCostEstimator {

  //TODO: Why does this class take a genotypesId param?
  
  val cheapStores: Set[Store] = Set(
      CoreStore.vcfFile, 
      CoreStore.sampleIdsFile, 
      CoreStore.vdsFile)
  
  val cheapTools: Set[ToolBase] = Set(
      CoreTool.checkPreExistingVcfFile(genotypesId), 
      CoreTool.extractSampleIdsFromVcfFile,
      CoreTool.importVcf)
  
  val expensiveStores: Set[Store] = Set(
      MiniMockStore.genotypesCassandraTable, 
      MiniMockStore.sampleIdsCassandraTable)
  
  val expensiveTools: Set[ToolBase] = Set(
      MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypesId),
      MiniMockTool.extractSampleIdsFromCassandraTable)

  val lowCost: Int = 100
  val mediumCost: Int = 200
  val highCost: Int = 300

  //TODO: TEST
  private[minimal] def costOf(store: Store): Int = {
    if (cheapStores.contains(store)) { lowCost }
    else if (expensiveStores.contains(store)) { highCost }
    else { mediumCost }
  }
  
  //TODO: TEST
  private[minimal] def costOf(tool: ToolBase): Int = {
    if (cheapTools.contains(tool)) { lowCost }
    else if (expensiveTools.contains(tool)) { highCost }
    else { mediumCost }
  }
  
  override def estimateCost(mapping: LToolMapping): Double = {
    val storeCosts = mapping.stores.values.map(costOf).sum
    
    val toolCosts = mapping.tools.values.map(costOf).sum
    
    storeCosts + toolCosts
  }
}
