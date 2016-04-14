package loamstream.apps.minimal

import loamstream.cost.LPipelineCostEstimator
import loamstream.map.LToolMapping
import loamstream.model.jobs.tools.LTool
import loamstream.model.stores.LStore
import loamstream.tools.core.CoreStore
import loamstream.tools.core.CoreTool

/**
  * LoamStream
  * Created by oliverr on 2/22/2016.
  */
case class LPipelineMiniCostEstimator(genotypesId: String) extends LPipelineCostEstimator {
  val cheapStores = Set[LStore](CoreStore.vcfFile, CoreStore.sampleIdsFile, CoreStore.vdsFile)
  val cheapTools =
    Set[LTool](CoreTool.checkPreExistingVcfFile(genotypesId), CoreTool.extractSampleIdsFromVcfFile,
      CoreTool.importVcf)
  val expensiveStores = Set[LStore](MiniMockStore.genotypesCassandraTable, MiniMockStore.sampleIdsCassandraTable)
  val expensiveTools =
    Set[LTool](MiniMockTool.checkPreExistingGenotypeCassandraTable(genotypesId),
      MiniMockTool.extractSampleIdsFromCassandraTable)

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
