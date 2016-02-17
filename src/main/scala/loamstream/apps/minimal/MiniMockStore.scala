package loamstream.apps.minimal

import loamstream.apps.minimal.MiniPipeline.{GenotypeCall, VariantId}
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPile, LSig}

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object MiniMockStore {

  val vcfFile =
    MiniMockStore(LPile(LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.vcfFile), "A VCF file.")
  val genotypesCassandraTable =
    MiniMockStore(LPile(LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.genotypesCassandraTable),
      "A Cassandra table of genotype calls")
  val sampleIdsFile =
    MiniMockStore(LPile(LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsFile), "A text file with sample ids")
  val sampleIdsCassandraTable =
    MiniMockStore(LPile(LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsCassandraTable),
      "A Cassandra table with sample ids.")
}

case class MiniMockStore(pile: LPile, comment: String)
