package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object PileKinds {

  val variantKeyIndexInGenotypes = 0
  val sampleKeyIndexInGenotypes = 1

  val genotypeCallsByVariantAndSample: LKind = LSpecificKind("genotypes by sample and variant")
  val sampleIds: LKind = LSpecificKind("sample ids")
  val pcaWeights: LKind = LSpecificKind("PCA weights")
  val pcaProjected: LKind = LSpecificKind("PCA projected")
  val sampleClustersByAncestry: LKind = LSpecificKind("sample clusters by ancestry")
  val singletonCounts: LKind = LSpecificKind("singleton counts")
}
