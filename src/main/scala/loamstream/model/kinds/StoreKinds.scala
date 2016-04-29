package loamstream.model.kinds


/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object StoreKinds {
  val variantKeyIndexInGenotypes: Int = 0
  val sampleKeyIndexInGenotypes: Int = 1

  val genotypeCallsByVariantAndSample: LKind = LSpecificKind("genotypes by sample and variant")
  val sampleIds: LKind = LSpecificKind("sample ids")
  val pcaWeights: LKind = LSpecificKind("PCA weights")
  val pcaProjected: LKind = LSpecificKind("PCA projected")
  val sampleClustersByAncestry: LKind = LSpecificKind("sample clusters by ancestry")
  val singletonCounts: LKind = LSpecificKind("singleton counts")
  
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
