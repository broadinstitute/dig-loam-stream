package loamstream.tools.core

import loamstream.model.LId
import loamstream.model.kinds.StoreKinds
import loamstream.model.StoreSpec
import loamstream.model.LSig
import loamstream.model.values.LType.LTuple.{LTuple1, LTuple2}
import loamstream.model.values.LType.{LClusterId, LDouble, LGenotype, LInt, LSampleId, LSingletonCount, LVariantId}
import loamstream.Sigs
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * @author oliverr 
  * @date 2/16/2016.
  * @author Clint
  */
//TODO rename? ConcreteStore?  
object CoreStore {

  def apply(name: String, sig: LSig, kind: LKind): StoreSpec = StoreSpec(sig, kind)
  
  def apply(sig: LSig, kind: LKind): StoreSpec = StoreSpec(sig, kind)
  
  import Sigs._
  
  val vcfFile: StoreSpec = {
    StoreSpec(variantAndSampleToGenotype, StoreKinds.vcfFile)
  }
    
  val vdsFile: StoreSpec = {
    StoreSpec(variantAndSampleToGenotype, StoreKinds.vdsFile)
  }
    
  val pcaWeightsFile: StoreSpec = {
    StoreSpec(sampleIdAndIntToDouble, StoreKinds.pcaWeightsFile)
  }
  
  val pcaProjectedFile: StoreSpec = {
    StoreSpec(sampleIdAndIntToDouble, StoreKinds.pcaProjectedFile)
  }
  
  val sampleClusterFile: StoreSpec  = {
    StoreSpec(LSampleId to LClusterId, StoreKinds.sampleClustersFile)
  }
  
  val singletonsFile: StoreSpec = {
    StoreSpec(sampleToSingletonCount, StoreKinds.singletonsFile)
  }
  
  val sampleIdsFile: StoreSpec = {
    StoreSpec(Sigs.sampleIds, StoreKinds.sampleIdsFile)
  }

  val stores: Set[StoreSpec] = Set(
      vcfFile, vdsFile, pcaWeightsFile, pcaProjectedFile, 
      sampleClusterFile, singletonsFile, sampleIdsFile)
}
