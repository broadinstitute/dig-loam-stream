package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind
import loamstream.model.kinds.instances.RecipeKinds.{calculateSingletons, calculateSingletonsFromGenotypeCalls,
  clusteringSamplesByFeatures, extractKey, extractSampleIdsFromGenotypeCalls, importVcf, loadVdsFromGenotypeCalls,
  pcaProjection, usePreExisting}
import loamstream.model.values.LType.LString
import loamstream.model.values.LType.LTuple.LTuple2

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object ToolKinds {
  def usePreExistingVCFFile(id: String): LSpecificKind[(String, String)] =
    LSpecificKind(LTuple2(LString, LString)("Use pre-existing VCF file", id), usePreExisting(id))

  def usePreExistingCassandraGenotypeCallsTable(id: String): LSpecificKind[(String, String)] =
    LSpecificKind(LTuple2(LString, LString)("Use pre-existing Cassandra genotype calls table", id),
      usePreExisting(id))

  val extractSampleIdsFromVcfFile =
    LSpecificKind("Extract sample ids from VCF file", extractKey(0), extractSampleIdsFromGenotypeCalls)
  val extractSampleIdsFromCassandraGenotypeCallsTable =
    LSpecificKind("Extract sample ids from Cassandra genotype calls table", extractKey(0),
      extractSampleIdsFromGenotypeCalls)
  val convertVcfToVds =
    LSpecificKind("Import VCF file into VDS format", importVcf(0), loadVdsFromGenotypeCalls)
  val calculateSingletonsFromVdsFile =
    LSpecificKind("Calculate singletons from VDS", calculateSingletons(0), calculateSingletonsFromGenotypeCalls)
  val nativePcaProjection = LSpecificKind("PCA projection (native method)", pcaProjection)
  val klustakwikClustering = LSpecificKind("Cluster samples using KlustaKwik", clusteringSamplesByFeatures)
}
