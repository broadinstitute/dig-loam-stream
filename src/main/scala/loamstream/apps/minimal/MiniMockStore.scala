package loamstream.apps.minimal

import loamstream.model.LId
import loamstream.model.kinds.StoreKinds
import loamstream.model.piles.{LPileSpec}
import loamstream.model.LSig
import loamstream.model.values.LType.LTuple.{LTuple1, LTuple2}
import loamstream.model.values.LType.{LGenotype, LSampleId, LString, LVariantId}
import loamstream.model.Store
import loamstream.tools.core.CoreStore
import loamstream.Sigs

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
object MiniMockStore {

  val genotypesCassandraTable: Store = CoreStore(
      "Cassandra genotype calls table", 
      LPileSpec(Sigs.variantAndSampleToGenotype, StoreKinds.genotypesCassandraTable))
      
  val sampleIdsCassandraTable: Store = CoreStore(
      "Cassandra sample ids table.", 
      LPileSpec(Sigs.setOf(LSampleId), StoreKinds.sampleIdsCassandraTable))

  val stores = Set[Store](genotypesCassandraTable, sampleIdsCassandraTable)
}
