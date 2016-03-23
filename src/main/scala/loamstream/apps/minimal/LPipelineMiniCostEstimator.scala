package loamstream.apps.minimal

import loamstream.cost.LPipelineCostEstimator
import loamstream.map.LToolMapping
import loamstream.model.jobs.tools.LTool
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
object LPipelineMiniCostEstimator extends LPipelineCostEstimator {
  val cheapStores = Set[LStore](MiniStore.vcfFile, MiniStore.sampleIdsFile, MiniStore.vdsFile)
  val cheapTools =
    Set[LTool](MiniTool.checkPreExistingVcfFile, MiniTool.extractSampleIdsFromVcfFile, MiniTool.importVcf)
  val expensiveStores = Set[LStore](MiniStore.genotypesCassandraTable, MiniStore.sampleIdsCassandraTable)
  val expensiveTools =
    Set[LTool](MiniTool.checkPreExistingGenotypeCassandraTable, MiniTool.extractSampleIdsFromCassandraTable)

  val lowCost = 100
  val mediumCost = 200
  val highCost = 300

  override def estimateCost(mapping: LToolMapping): Double = {
    val storeCosts = mapping.stores.values.map(store =>
      if (cheapStores.contains(store)) {
        lowCost
      }
      else if (expensiveStores.contains(store)) {
        highCost
      }
      else {
        mediumCost
      }
    ).sum
    val toolCosts = mapping.tools.values.map(tool =>
      if (cheapTools.contains(tool)) {
        lowCost
      }
      else if (expensiveTools.contains(tool)) {
        highCost
      }
      else {
        mediumCost
      }
    ).sum
    storeCosts + toolCosts
  }
}
