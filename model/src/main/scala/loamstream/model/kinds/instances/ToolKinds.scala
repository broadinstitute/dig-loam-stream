package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind
import loamstream.model.kinds.instances.RecipeKinds.{extractKey, extractSampleIdsFromGenotypeCalls, usePreExisting}

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object ToolKinds {
  val usePreExistingVCFFile = LSpecificKind("Use pre-existing VCF file", usePreExisting)
  val usePreExistingCassandraGenotypeCallsTable =
    LSpecificKind("Use pre-existing Cassandra genotype calls table", usePreExisting)
  val extractSampleIdsFromVCFFile =
    LSpecificKind("Extract sample ids from VCF file", extractKey(0), extractSampleIdsFromGenotypeCalls)
  val extractSampleIdsFromCassandraGenotypeCallsTable =
    LSpecificKind("Extract sample ids from Cassandra genotype calls table", extractKey(0),
      extractSampleIdsFromGenotypeCalls)
}
