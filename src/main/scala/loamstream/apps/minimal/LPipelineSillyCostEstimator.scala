package loamstream.apps.minimal

import loamstream.cost.LPipelineCostEstimator
import loamstream.map.LToolMapping
import loamstream.model.jobs.tools.LTool
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
object LPipelineSillyCostEstimator extends LPipelineCostEstimator {
  val cheapStores = Set[LStore](MiniMockStore.vcfFile, MiniMockStore.sampleIdsFile)
  val cheapTools = Set[LTool](MiniMockTool.checkPreExistingVcfFile, MiniMockTool.extractSampleIdsFromVcfFile)
  val expensiveStores = Set[LStore](MiniMockStore.genotypesCassandraTable, MiniMockStore.sampleIdsCassandraTable)
  val expensiveTools =
    Set[LTool](MiniMockTool.checkPreExistingGenotypeCassandraTable, MiniMockTool.extractSampleIdsFromCassandraTable)

  val lowCost = 100
  val mediumCost = 200
  val highCost = 300

  override def estimateCost(mapping: LToolMapping): Double = {
    val storeCosts = mapping.stores.values.map(store =>
      if (cheapStores.contains(store)) lowCost
      else if (expensiveStores.contains(store)) highCost
      else mediumCost
    ).sum
    val toolCosts = mapping.tools.values.map(tool =>
      if (cheapTools.contains(tool)) lowCost
      else if (expensiveTools.contains(tool)) highCost
      else mediumCost
    ).sum
    storeCosts + toolCosts
  }
}
