package loamstream.tools.core

import loamstream.Sigs
import loamstream.model.kinds.{LKind, StoreKinds}
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

  def apply(name: String, sig: LSig, kind: LKind): CoreStore = CoreStore(name, StoreSpec(sig, kind))

  def apply(sig: LSig, kind: LKind): CoreStore = CoreStore(LId.newAnonId, StoreSpec(sig, kind))

  import Sigs._

  val vcfFile: Store = {
    CoreStore("VCF file", StoreSpec(variantAndSampleToGenotype, StoreKinds.vcfFile))
  }

  val vdsFile: Store = {
    CoreStore("VDS file", StoreSpec(variantAndSampleToGenotype, StoreKinds.vdsFile))
  }

  val pcaWeightsFile: Store = {
    CoreStore("PCA weights file", StoreSpec(sampleIdAndIntToDouble, StoreKinds.pcaWeightsFile))
  }

  val pcaProjectedFile: Store = {
    CoreStore("PCA projected file", StoreSpec(sampleIdAndIntToDouble, StoreKinds.pcaProjectedFile))
  }

  val sampleClusterFile: Store = {
    CoreStore("Sample cluster file", StoreSpec(LString to LInt, StoreKinds.sampleClustersFile))
  }

  val singletonsFile: Store = {
    CoreStore("Singletons file", StoreSpec(sampleToSingletonCount, StoreKinds.singletonsFile))
  }

  val sampleIdsFile: Store = {
    CoreStore("Sample ids file", StoreSpec(Sigs.sampleIds, StoreKinds.sampleIdsFile))
  }

  val stores: Set[Store] = Set(
    vcfFile, vdsFile, pcaWeightsFile, pcaProjectedFile,
    sampleClusterFile, /*singletonsFile, */ sampleIdsFile)
}
