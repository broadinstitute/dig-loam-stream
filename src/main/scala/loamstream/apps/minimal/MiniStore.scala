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

  val vcfFile =
    MiniStore(LPile(LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.vcfFile), "A VCF file.")
  val genotypesCassandraTable =
    MiniStore(LPile(LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.genotypesCassandraTable),
      "A Cassandra table of genotype calls")
  val sampleIdsFile =
    MiniStore(LPile(LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsFile), "A text file with sample ids")
  val sampleIdsCassandraTable =
    MiniStore(LPile(LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsCassandraTable),
      "A Cassandra table with sample ids.")

  val stores = Set[LStore](vcfFile, genotypesCassandraTable, sampleIdsFile, sampleIdsCassandraTable)
}

case class MiniStore(pile: LPile, comment: String) extends LStore
