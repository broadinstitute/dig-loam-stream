package loamstream.model.kinds.instances

import loamstream.model.kinds.LNamedKind
import loamstream.model.kinds.instances.RecipeKinds.{extractFirstKey, extractSampleIdsFromGenotypeCalls, usePreExisting}

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object ToolKinds {
  val usePreExistingVCFFile = LNamedKind("Use pre-existing VCF file", usePreExisting)
  val usePreExistingCassandraGenotypeCallsTable =
    LNamedKind("Use pre-existing Cassandra genotype calls table", usePreExisting)
  val extractSampleIdsFromVCFFile =
    LNamedKind("Extract sample ids from VCF file", extractFirstKey, extractSampleIdsFromGenotypeCalls)
  val extractSampleIdsFromCassandraGenotypeCallsTable =
    LNamedKind("Extract sample ids from Cassandra genotype calls table", extractFirstKey,
      extractSampleIdsFromGenotypeCalls)
}
