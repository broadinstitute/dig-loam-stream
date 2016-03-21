package loamstream.pipelines.qc.ancestry

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.kinds.instances.PileKinds
import loamstream.model.piles.{LPile, LSig}

/**
  * LoamStream
  * Created by oliverr on 3/21/2016.
  */
object AncestryInferencePipeline {

  type SampleId = String
  type VariantId = String

  val genotypesPileId = "genotypes"
  val pcaWeightsPileId = "pcaWeights"

  val genotypesPile =
    LPile(genotypesPileId, LSig.Map[(VariantId, SampleId), Genotype].get, PileKinds.genotypeCallsByVariantAndSample)

}
