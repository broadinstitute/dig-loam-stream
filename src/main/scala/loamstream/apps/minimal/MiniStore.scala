package loamstream.apps.minimal

import loamstream.apps.minimal.MiniPipeline.{GenotypeCall, VariantId}
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPile, LSig}
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object MiniStore {

  val vcfFile = MiniStore(LPile("VCF file", LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.vcfFile))
  val genotypesCassandraTable =
    MiniStore(LPile("Cassandra genotype calls table", LSig.Map[(String, VariantId), GenotypeCall].get,
      StoreKinds.genotypesCassandraTable))
  val sampleIdsFile =
    MiniStore(LPile("Sample ids file", LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsFile))
  val sampleIdsCassandraTable =
    MiniStore(LPile("Cassandra sample ids table.", LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsCassandraTable))

  val stores = Set[LStore](vcfFile, genotypesCassandraTable, sampleIdsFile, sampleIdsCassandraTable)
}

case class MiniStore(pile: LPile) extends LStore
