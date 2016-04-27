package loamstream.apps.minimal

import loamstream.model.id.LId
import loamstream.model.id.LId.LNamedId
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.recipes.LRecipeSpec
import loamstream.model.ToolBase
import loamstream.tools.core.StoreOps
import loamstream.tools.core.CoreTool

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
object MiniMockTool {
  
  import StoreOps._
  
  def checkPreExistingGenotypeCassandraTable(tableId: String): ToolBase = CoreTool.nullaryTool(
      tableId, 
      "What a nice table on Cassandra full of genotype calls!", 
      MiniMockStore.genotypesCassandraTable, 
      LRecipeSpec.preExistingCheckout(tableId))

  val extractSampleIdsFromCassandraTable: ToolBase = CoreTool.unaryTool(
      "Extracted sample ids from Cassandra genotype calls table into another table.", 
      MiniMockStore.genotypesCassandraTable ~> MiniMockStore.sampleIdsCassandraTable,
      LRecipeSpec.keyExtraction(PileKinds.sampleKeyIndexInGenotypes) _)

  def tools(tableId: String): Set[ToolBase] = {
    Set(checkPreExistingGenotypeCassandraTable(tableId), extractSampleIdsFromCassandraTable)
  }
}
