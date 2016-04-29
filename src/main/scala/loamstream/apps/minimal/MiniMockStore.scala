package loamstream.apps.minimal

import loamstream.Sigs
import loamstream.model.Store
import loamstream.model.StoreSpec
import loamstream.model.kinds.StoreKinds
import loamstream.model.values.LType.LSampleId
import loamstream.tools.core.CoreStore

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
object MiniMockStore {

  val genotypesCassandraTable: Store = CoreStore(
      "Cassandra genotype calls table", 
      StoreSpec(Sigs.variantAndSampleToGenotype, StoreKinds.genotypesCassandraTable))
      
  val sampleIdsCassandraTable: Store = CoreStore(
      "Cassandra sample ids table.", 
      StoreSpec(Sigs.setOf(LSampleId), StoreKinds.sampleIdsCassandraTable))

  val stores = Set[Store](genotypesCassandraTable, sampleIdsCassandraTable)
}
