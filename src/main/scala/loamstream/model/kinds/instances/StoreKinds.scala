package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind
import loamstream.model.kinds.instances.PileKinds._
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object StoreKinds {
  val vcfFile: LKind = LSpecificKind("VCF file", genotypeCallsByVariantAndSample)
  val vdsFile: LKind = LSpecificKind("VDS file", genotypeCallsByVariantAndSample)
  val pcaWeightsFile: LKind = LSpecificKind("PCA weights file", pcaWeights)
  val pcaProjectedFile: LKind = LSpecificKind("PCA projected values file", pcaProjected)
  val sampleClustersFile: LKind = LSpecificKind("sample clusters files", sampleClustersByAncestry)

  val singletonsFile: LKind = LSpecificKind("Singletons file", singletonCounts)
  val genotypesCassandraTable: LKind = LSpecificKind("Genotypes Cassandra table", genotypeCallsByVariantAndSample)
  val sampleIdsFile: LKind = LSpecificKind("Sample ids file", sampleIds)
  val sampleIdsCassandraTable: LKind = LSpecificKind("Sample ids Cassandra table", sampleIds)
}
