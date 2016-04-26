package loamstream.apps.minimal

import loamstream.model.id.LId
import loamstream.model.id.LId.LNamedId
import loamstream.model.jobs.tools.LTool
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.recipes.LRecipeSpec

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
case class MiniMockTool(id: LId, recipe: LRecipeSpec) extends LTool

object MiniMockTool {

  def apply(name: String, recipe: LRecipeSpec): MiniMockTool = MiniMockTool(LNamedId(name), recipe)
  
  def checkPreExistingGenotypeCassandraTable(tableId: String): MiniMockTool =
    MiniMockTool("What a nice table on Cassandra full of genotype calls!", LRecipeSpec.preExistingCheckout(tableId,
      MiniMockStore.genotypesCassandraTable.spec))

  val extractSampleIdsFromCassandraTable =
    MiniMockTool("Extracted sample ids from Cassandra genotype calls table into another table.",
      LRecipeSpec.keyExtraction(MiniMockStore.genotypesCassandraTable.spec,
        MiniMockStore.sampleIdsCassandraTable.spec, PileKinds.sampleKeyIndexInGenotypes))

  def tools(tableId: String): Set[LTool] = {
    Set(checkPreExistingGenotypeCassandraTable(tableId), extractSampleIdsFromCassandraTable)
  }
}
