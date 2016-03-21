package loamstream.apps.minimal

import loamstream.model.id.LId
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPileSpec, LSig}
import loamstream.model.signatures.Signatures.{SingletonCount, GenotypeCall, VariantId}
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object MiniStore {

  val vcfFile = MiniStore("VCF file", LPileSpec(LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.vcfFile))
  val vdsFile = MiniStore("VDS file", LPileSpec(LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.vdsFile))
  val singletonsFile = MiniStore("Singletons file",
    LPileSpec(LSig.Map[Tuple1[String], SingletonCount].get, StoreKinds.singletonsFile))
  val genotypesCassandraTable = MiniStore("Cassandra genotype calls table",
      LPileSpec(LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.genotypesCassandraTable))
  val sampleIdsFile = MiniStore("Sample ids file", LPileSpec(LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsFile))
  val sampleIdsCassandraTable = MiniStore("Cassandra sample ids table.",
      LPileSpec(LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsCassandraTable))

  val stores = Set[LStore](vcfFile, vdsFile, singletonsFile, genotypesCassandraTable, sampleIdsFile,
    sampleIdsCassandraTable)

  def apply(name: String, pile: LPileSpec): MiniStore = MiniStore(LId.LNamedId(name), pile)
}

case class MiniStore(id: LId, pile: LPileSpec) extends LStore
