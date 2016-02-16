package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind
import loamstream.model.kinds.instances.PileKinds.{genotypeCallsBySampleAndVariant, sampleIds}

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object StoreKinds {
  val vcfFile = LSpecificKind("VCF file", genotypeCallsBySampleAndVariant)
  val genotypesCassandraTable = LSpecificKind("Genotypes Cassandra table", genotypeCallsBySampleAndVariant)
  val sampleIdsFile = LSpecificKind("Sample ids file", sampleIds)
  val sampleIdsCassandraTable = LSpecificKind("Sample ids file", sampleIds)
}
