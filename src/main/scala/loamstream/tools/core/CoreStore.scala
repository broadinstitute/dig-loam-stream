package loamstream.tools.core

import loamstream.Sigs
import loamstream.model.{LId, Store, StoreSpec}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
//TODO rename? ConcreteStore?  
final case class CoreStore(id: LId, spec: StoreSpec) extends Store

object CoreStore {

  def apply(name: String, spec: StoreSpec): CoreStore = CoreStore(LId.LNamedId(name), spec)

  def apply(name: String, sig: Type): CoreStore = CoreStore(name, StoreSpec(sig))

  def apply(sig: Type): CoreStore = CoreStore(LId.newAnonId, StoreSpec(sig))

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
    CoreStore("Sample cluster file", StoreSpec(typeOf[Map[String, Int]]))
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
