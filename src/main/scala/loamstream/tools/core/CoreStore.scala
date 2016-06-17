package loamstream.tools.core

import loamstream.Sigs
import loamstream.model.{LId, Store, StoreSig}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
//TODO rename? ConcreteStore?  
final case class CoreStore(id: LId, sig: StoreSig) extends Store

object CoreStore {

  def apply(name: String, spec: StoreSig): CoreStore = CoreStore(LId.LNamedId(name), spec)

  def apply(name: String, sig: Type): CoreStore = CoreStore(name, new StoreSig(sig))

  def apply(sig: Type): CoreStore = CoreStore(LId.newAnonId, new StoreSig(sig))

  import Sigs._

  val vcfFile: Store = {
    CoreStore("VCF file", new StoreSig(variantAndSampleToGenotype))
  }

  val vdsFile: Store = {
    CoreStore("VDS file", new StoreSig(variantAndSampleToGenotype))
  }

  val pcaWeightsFile: Store = {
    CoreStore("PCA weights file", new StoreSig(sampleIdAndIntToDouble))
  }
  val pcaProjectedFile: Store = {
    CoreStore("PCA projected file", new StoreSig(sampleIdAndIntToDouble))
  }

  val sampleClusterFile: Store = {
    CoreStore("Sample cluster file", new StoreSig(typeOf[Map[String, Int]]))
  }

  val singletonsFile: Store = {
    CoreStore("Singletons file", new StoreSig(sampleToSingletonCount))
  }

  val sampleIdsFile: Store = {
    CoreStore("Sample ids file", new StoreSig(Sigs.sampleIds))
  }

  val stores: Set[Store] = Set(
    vcfFile, vdsFile, pcaWeightsFile, pcaProjectedFile,
    sampleClusterFile, singletonsFile, sampleIdsFile)
}
