package loamstream.model.kinds.instances

import loamstream.model.kinds.LNamedKind
import loamstream.model.kinds.instances.PileKinds.{genotypeCallsBySampleAndVariant, sampleIds}

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
class StoreKinds {
  val vcfFile = LNamedKind("VCF file", genotypeCallsBySampleAndVariant)
  val genotypesCassandraTable = LNamedKind("Genotypes Cassandra table", genotypeCallsBySampleAndVariant)
  val sampleIdsFile = LNamedKind("Sample ids file", sampleIds)
  val sampleIdsCassandraTable = LNamedKind("Sample ids file", sampleIds)
}
