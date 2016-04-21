package loamstream.apps.minimal

import loamstream.model.id.LId
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPileSpec, LSig}
import loamstream.model.stores.LStore
import loamstream.model.values.LType.{LGenotype, LSampleId, LTuple, LVariantId}

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
object MiniMockStore {

  val genotypesCassandraTable =
    MiniMockStore("Cassandra genotype calls table", LPileSpec(LSig.Map(LTuple(LVariantId, LSampleId), LGenotype),
      StoreKinds.genotypesCassandraTable))
  val sampleIdsCassandraTable =
    MiniMockStore("Cassandra sample ids table.", LPileSpec(LSig.Set(LTuple(LSampleId)),
      StoreKinds.sampleIdsCassandraTable))

  val stores = Set[LStore](genotypesCassandraTable, sampleIdsCassandraTable)

  def apply(name: String, pile: LPileSpec): MiniMockStore = MiniMockStore(LId.LNamedId(name), pile)
}

case class MiniMockStore(id: LId, pile: LPileSpec) extends LStore
