package loamstream.apps.minimal

import loamstream.Sigs
import loamstream.model.StoreSpec
import loamstream.model.kinds.StoreKinds
import loamstream.model.values.LType.LSampleId
import loamstream.tools.core.CoreStore

/**
  * LoamStream
  * Created by oliverr on 3/29/2016.
  */
object MiniMockStore {

  val genotypesCassandraTable: StoreSpec = {
    StoreSpec(Sigs.variantAndSampleToGenotype, StoreKinds.genotypesCassandraTable)
  }
      
  val sampleIdsCassandraTable: StoreSpec = {
    StoreSpec(Sigs.setOf(LSampleId), StoreKinds.sampleIdsCassandraTable)
  }

  val stores = Set[StoreSpec](genotypesCassandraTable, sampleIdsCassandraTable)
}
