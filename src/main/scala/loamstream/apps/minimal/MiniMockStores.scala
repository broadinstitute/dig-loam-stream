package loamstream.apps.minimal

import loamstream.apps.minimal.MiniApp.{GenotypeCall, VariantId}
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LSig, LPile}

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object MiniMockStores {

  val vcfFile = LPile(LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.vcfFile)
  val genotypesCassandraTable =
    LPile(LSig.Map[(String, VariantId), GenotypeCall].get, StoreKinds.genotypesCassandraTable)
  val sampleIdsFile = LPile(LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsFile)
  val sampleIdsCassandraTable = LPile(LSig.Set[Tuple1[String]].get, StoreKinds.sampleIdsCassandraTable)
}
