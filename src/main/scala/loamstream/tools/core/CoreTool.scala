package loamstream.tools.core

import loamstream.LEnv
import loamstream.model.LId
import loamstream.model.LId.LNamedId
import loamstream.model.ToolSpec
import LCoreEnv._
import loamstream.model.Tool
import loamstream.model.Store
import loamstream.model.StoreSpec
import java.nio.file.Path
import loamstream.model.FileStore
import loamstream.tools.klusta.KlustaKwikKonfig
import loamstream.model.StoreOps.UnarySig
import loamstream.model.StoreOps.NarySig
import loamstream.model.StoreOps.BinarySig
import loamstream.model.StoreOps

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
final case class CoreTool(id: LId, spec: ToolSpec, inputs: Map[LId, Store], outputs: Map[LId, Store]) extends Tool

object CoreTool {
  
  import StoreOps._
  import FileStore.{ 
    vcfFile => vcfStore, 
    vdsFile => vdsStore, 
    singletonsFile => singletonsStore, 
    sampleIdsFile => sampleIdsStore,
    pcaWeightsFile => pcaWeightsStore,
    pcaProjectedFile => pcaProjectedStore,
    sampleClusterFile => sampleClusterStore
  }
  
  final case class CheckPreExistingVcfFile(vcfFile: Path) extends Tool.CheckPreexisting(vcfFile, vcfStore(vcfFile))
  
  final case class CheckPreExistingPcaWeightsFile(pcaWeightsFile: Path) extends 
      Tool.CheckPreexisting(pcaWeightsFile, pcaWeightsStore(pcaWeightsFile))
  
  final case class ExtractSampleIdsFromVcfFile(
      vcfFile: Path, 
      sampleFile: Path) extends Tool.OneToOne(vcfStore(vcfFile) ~> sampleIdsStore(sampleFile)) {
    
    override val id: LId = LNamedId(s"Extracting sample ids from '$vcfFile' and storing them in '$sampleFile'")
  }
  
  final case class ConvertVcfToVds(
      vcfFile: Path, 
      vdsDir: Path) extends Tool.OneToOne(vcfStore(vcfFile) ~> vdsStore(vdsDir)) {
    
    override val id: LId = LNamedId(s"Convert '$vcfFile' into Hail format VDS dir '$vdsDir'.")
  }

  final case class CalculateSingletons(
      vdsDir: Path, 
      singletonsFile: Path) extends Tool.OneToOne(vdsStore(vdsDir) ~> singletonsStore(singletonsFile)) {
    
    override val id: LId = LNamedId(s"Calculate singletons from genotype calls in VDS format at '$vdsDir'.")
  }
  
  final case class ProjectPcaNative(vcfFile: Path, pcaWeightsFile: Path, klustaConfig: KlustaKwikKonfig) extends 
      Tool.Nary((vcfStore(vcfFile), pcaWeightsStore(pcaWeightsFile)) ~> pcaProjectedStore(klustaConfig.inputFile)) {
    
    override val id: LId = {
      LNamedId(s"Project PCA using native method: vcf: '$vcfFile', pca weights: '$pcaWeightsFile'.")
    }
  }
  
  final case class ProjectPca(vcfFile: Path, pcaWeightsFile: Path, klustaConfig: KlustaKwikKonfig) extends 
      Tool.Nary((vcfStore(vcfFile), pcaWeightsStore(pcaWeightsFile)) ~> pcaProjectedStore(klustaConfig.inputFile)) {
    
    override val id: LId = LNamedId(s"Project PCA: vcf: '$vcfFile', pca weights: '$pcaWeightsFile'.")
  }
  
  final case class KlustaKwikClustering(klustaConfig: KlustaKwikKonfig) extends 
      Tool.OneToOne(pcaProjectedStore(klustaConfig.inputFile) ~> sampleClusterStore(klustaConfig.outputFile)) {
    
    override val id: LId = {
      val input = klustaConfig.inputFile
      val output = klustaConfig.outputFile
      
      LNamedId(s"KlustaKwik Clustering: pca projected file '$input', sample cluster file: '$output'.")
    }
  }
  
  final case class ClusteringSamplesByFeatures(klustaConfig: KlustaKwikKonfig) extends 
      Tool.OneToOne(pcaProjectedStore(klustaConfig.inputFile) ~> sampleClusterStore(klustaConfig.outputFile)) {
    
    override val id: LId = {
      val input = klustaConfig.inputFile
      val output = klustaConfig.outputFile
      
      LNamedId(s"Clustering Samples by Features: pca projected file '$input', sample cluster file: '$output'.")
    }
  }
}
