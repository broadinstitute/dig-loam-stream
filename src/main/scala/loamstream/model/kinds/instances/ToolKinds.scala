package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind
import loamstream.model.kinds.instances.RecipeKinds.{extractKey, extractSampleIdsFromGenotypeCalls, usePreExisting}

/**
 * LoamStream
 * Created by oliverr on 2/16/2016.
 */
object ToolKinds {
  def usePreExistingVCFFile(id: String): LSpecificKind[(String, String)] =
    LSpecificKind(("Use pre-existing VCF file", id), usePreExisting(id))

  def usePreExistingCassandraGenotypeCallsTable(id: String): LSpecificKind[(String, String)] =
    LSpecificKind(("Use pre-existing Cassandra genotype calls table", id), usePreExisting(id))

  val extractSampleIdsFromVCFFile =
    LSpecificKind("Extract sample ids from VCF file", extractKey(0), extractSampleIdsFromGenotypeCalls)
  val extractSampleIdsFromCassandraGenotypeCallsTable =
    LSpecificKind("Extract sample ids from Cassandra genotype calls table", extractKey(0),
      extractSampleIdsFromGenotypeCalls)
}
