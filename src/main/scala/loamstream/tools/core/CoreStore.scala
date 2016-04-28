package loamstream.tools.core

import loamstream.model.LId
import loamstream.model.kinds.StoreKinds
import loamstream.model.piles.{LPileSpec}
import loamstream.model.LSig
import loamstream.model.values.LType.LTuple.{LTuple1, LTuple2}
import loamstream.model.values.LType.{LClusterId, LDouble, LGenotype, LInt, LSampleId, LSingletonCount, LVariantId}
import loamstream.model.Store
import loamstream.Sigs
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
//TODo rename? ConcreteStore?  
final case class CoreStore(id: LId, spec: LPileSpec) extends Store

object CoreStore {

  def apply(name: String, spec: LPileSpec): CoreStore = CoreStore(LId.LNamedId(name), spec)
  
  def apply(name: String, sig: LSig, kind: LKind): CoreStore = CoreStore(name, LPileSpec(sig, kind))
  
  def apply(sig: LSig, kind: LKind): CoreStore = CoreStore(LId.newAnonId, LPileSpec(sig, kind))
  
  import Sigs._
  
  val vcfFile: Store  = {
    CoreStore("VCF file", LPileSpec(variantAndSampleToGenotype, StoreKinds.vcfFile))
  }
    
  val vdsFile: Store  = {
    CoreStore("VDS file", LPileSpec(variantAndSampleToGenotype, StoreKinds.vdsFile))
  }
    
  val pcaWeightsFile: Store = {
    CoreStore("PCA weights file", LPileSpec(sampleIdAndIntToDouble, StoreKinds.pcaWeightsFile))
  }
  
  val pcaProjectedFile: Store  = {
    CoreStore("PCA projected file", LPileSpec(sampleIdAndIntToDouble, StoreKinds.pcaProjectedFile))
  }
  
  val sampleClusterFile: Store  = {
    CoreStore("Sample cluster file", LPileSpec(LSampleId to LClusterId, StoreKinds.sampleClustersFile))
  }
  
  val singletonsFile: Store = {
    CoreStore("Singletons file", LPileSpec(sampleToSingletonCount, StoreKinds.singletonsFile))
  }
  
  val sampleIdsFile: Store = {
    CoreStore("Sample ids file", LPileSpec(LSig.Set.of(LSampleId), StoreKinds.sampleIdsFile))
  }

  val stores: Set[Store] = Set(
      vcfFile, vdsFile, pcaWeightsFile, pcaProjectedFile, 
      sampleClusterFile, singletonsFile, sampleIdsFile)
}
