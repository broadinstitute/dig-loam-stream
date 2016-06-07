package loamstream.tools.core

import loamstream.Sigs
import loamstream.model.values.LType.{LInt, LString}
import loamstream.model.{LId, LSig, Store, StoreSpec}

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
//TODO rename? ConcreteStore?  
final case class CoreStore(id: LId, spec: StoreSpec) extends Store

object CoreStore {

  def apply(name: String, spec: StoreSpec): CoreStore = CoreStore(LId.LNamedId(name), spec)

  def apply(name: String, sig: LSig): CoreStore = CoreStore(name, StoreSpec(sig))

  def apply(sig: LSig): CoreStore = CoreStore(LId.newAnonId, StoreSpec(sig))

  import Sigs._

  val vcfFile: Store = {
    CoreStore("VCF file", StoreSpec(variantAndSampleToGenotype))
  }

  val vdsFile: Store = {
    CoreStore("VDS file", StoreSpec(variantAndSampleToGenotype))
  }

  val pcaWeightsFile: Store = {
    CoreStore("PCA weights file", StoreSpec(sampleIdAndIntToDouble))
  }
  val pcaProjectedFile: Store = {
    CoreStore("PCA projected file", StoreSpec(sampleIdAndIntToDouble))
  }

  val sampleClusterFile: Store = {
    CoreStore("Sample cluster file", StoreSpec(LString to LInt))
  }

  val singletonsFile: Store = {
    CoreStore("Singletons file", StoreSpec(sampleToSingletonCount))
  }

  val sampleIdsFile: Store = {
    CoreStore("Sample ids file", StoreSpec(Sigs.sampleIds))
  }

  val stores: Set[Store] = Set(
    vcfFile, vdsFile, pcaWeightsFile, pcaProjectedFile,
    sampleClusterFile, singletonsFile, sampleIdsFile)
}
