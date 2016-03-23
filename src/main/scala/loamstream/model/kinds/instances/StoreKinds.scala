package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind
import loamstream.model.kinds.instances.PileKinds.{genotypeCallsBySampleAndVariant, sampleIds, singletonCounts}

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object StoreKinds {
  val vcfFile = LSpecificKind("VCF file", genotypeCallsBySampleAndVariant)
  val vdsFile = LSpecificKind("VDS file", genotypeCallsBySampleAndVariant)
  val singletonsFile = LSpecificKind("Singletons file", singletonCounts)
  val genotypesCassandraTable = LSpecificKind("Genotypes Cassandra table", genotypeCallsBySampleAndVariant)
  val sampleIdsFile = LSpecificKind("Sample ids file", sampleIds)
  val sampleIdsCassandraTable = LSpecificKind("Sample ids Cassandra table", sampleIds)
}
