package loamstream.apps.minimal

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.id.LId
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPileSpec, LSig}
import loamstream.model.signatures.Signatures.{SingletonCount, _}
import loamstream.model.stores.LStore

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
object MiniMockStore {

  val genotypesCassandraTable = MiniMockStore("Cassandra genotype calls table",
    LPileSpec(LSig.Map[(String, VariantId), Genotype], StoreKinds.genotypesCassandraTable))
  val sampleIdsCassandraTable = MiniMockStore("Cassandra sample ids table.", LPileSpec(LSig.Set[Tuple1[String]],
    StoreKinds.sampleIdsCassandraTable))

  val stores = Set[LStore](genotypesCassandraTable, sampleIdsCassandraTable)

  def apply(name: String, pile: LPileSpec): MiniMockStore = MiniMockStore(LId.LNamedId(name), pile)
}

case class MiniMockStore(id: LId, pile: LPileSpec) extends LStore
