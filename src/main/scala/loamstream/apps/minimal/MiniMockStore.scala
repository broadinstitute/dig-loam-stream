package loamstream.apps.minimal

import loamstream.model.id.LId
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPileSpec, LSig}
import loamstream.model.stores.LStore
import loamstream.model.values.LType.LTuple.{LTuple1, LTuple2}
import loamstream.model.values.LType.{LGenotype, LString, LVariantId}

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
object MiniMockStore {

  val genotypesCassandraTable =
    MiniMockStore("Cassandra genotype calls table", LPileSpec(LSig.Map(LTuple2(LString, LVariantId), LGenotype),
      StoreKinds.genotypesCassandraTable))
  val sampleIdsCassandraTable =
    MiniMockStore("Cassandra sample ids table.", LPileSpec(LSig.Set(LTuple1(LString)),
      StoreKinds.sampleIdsCassandraTable))

  val stores = Set[LStore](genotypesCassandraTable, sampleIdsCassandraTable)

  def apply(name: String, pile: LPileSpec): MiniMockStore = MiniMockStore(LId.LNamedId(name), pile)
}

case class MiniMockStore(id: LId, pile: LPileSpec) extends LStore
