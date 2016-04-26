package loamstream.tools.core

import loamstream.model.id.LId
import loamstream.model.kinds.instances.StoreKinds
import loamstream.model.piles.{LPileSpec, LSig}
import loamstream.model.values.LType.LTuple.{LTuple1, LTuple2}
import loamstream.model.values.LType.{LClusterId, LDouble, LGenotype, LInt, LSampleId, LSingletonCount, LVariantId}
import loamstream.model.StoreBase
import loamstream.Sigs
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
//TODo rename? ConcreteStore?  
final case class CoreStore(id: LId, spec: LPileSpec) extends StoreBase

object CoreStore {

  def apply(name: String, spec: LPileSpec): CoreStore = CoreStore(LId.LNamedId(name), spec)
  
  def apply(name: String, sig: LSig, kind: LKind): CoreStore = CoreStore(name, LPileSpec(sig, kind))
  
  def apply(sig: LSig, kind: LKind): CoreStore = CoreStore(LId.newAnonId, LPileSpec(sig, kind))
  
  import Sigs._
  
  val vcfFile: StoreBase  = {
    CoreStore("VCF file", LPileSpec(variantAndSampleToGenotype, StoreKinds.vcfFile))
  }
    
  val vdsFile: StoreBase  = {
    CoreStore("VDS file", LPileSpec(variantAndSampleToGenotype, StoreKinds.vdsFile))
  }
    
  val pcaWeightsFile: StoreBase = {
    CoreStore("PCA weights file", LPileSpec(sampleIdAndIntToDouble, StoreKinds.pcaWeightsFile))
  }
  
  val pcaProjectedFile: StoreBase  = {
    CoreStore("PCA projected file", LPileSpec(sampleIdAndIntToDouble, StoreKinds.pcaProjectedFile))
  }
  
  val sampleClusterFile: StoreBase  = {
    CoreStore("Sample cluster file", LPileSpec(LSampleId to LClusterId, StoreKinds.sampleClustersFile))
  }
  
  val singletonsFile: StoreBase = {
    CoreStore("Singletons file", LPileSpec(LSampleId to LSingletonCount, StoreKinds.singletonsFile))
  }
  
  val sampleIdsFile: StoreBase = {
    CoreStore("Sample ids file", LPileSpec(LSig.Set.of(LSampleId), StoreKinds.sampleIdsFile))
  }

  val stores: Set[StoreBase] = Set(
      vcfFile, vdsFile, pcaWeightsFile, pcaProjectedFile, 
      sampleClusterFile, singletonsFile, sampleIdsFile)
}
