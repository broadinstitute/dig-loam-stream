package loamstream.apps.minimal

import loamstream.model.Tool
import loamstream.model.kinds.StoreKinds
import loamstream.model.recipes.LRecipeSpec
import loamstream.tools.core.CoreTool
import loamstream.tools.core.StoreOps

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
object MiniMockTool {
  
  import StoreOps._
  
  def checkPreExistingGenotypeCassandraTable(tableId: String): Tool = CoreTool.nullaryTool(
      tableId, 
      "What a nice table on Cassandra full of genotype calls!", 
      MiniMockStore.genotypesCassandraTable, 
      LRecipeSpec.preExistingCheckout(tableId))

  val extractSampleIdsFromCassandraTable: Tool = CoreTool.unaryTool(
      "Extracted sample ids from Cassandra genotype calls table into another table.", 
      MiniMockStore.genotypesCassandraTable ~> MiniMockStore.sampleIdsCassandraTable,
      LRecipeSpec.keyExtraction(StoreKinds.sampleKeyIndexInGenotypes) _)

  def tools(tableId: String): Set[Tool] = {
    Set(checkPreExistingGenotypeCassandraTable(tableId), extractSampleIdsFromCassandraTable)
  }
}
