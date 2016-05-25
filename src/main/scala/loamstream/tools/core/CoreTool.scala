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

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
final case class CoreTool(id: LId, spec: ToolSpec, inputs: Map[LId, Store], outputs: Map[LId, Store]) extends Tool

object CoreTool {
  
  def apply(name: String, toolSpec: ToolSpec, inputs: Map[LId, Store], outputs: Map[LId, Store]): CoreTool = {
    CoreTool(LNamedId(name), toolSpec, inputs, outputs)
  }
  
  import StoreOps._

  abstract class CheckPreexistingTool(file: Path, outputStore: Store) extends Tool with Tool.NoInputs {
    override val id = LId.LNamedId(file.toString)
    
    override val spec: ToolSpec = ToolSpec.producing(outputStore.spec) 
  
    override val outputs: Map[LId, Store] = Map(ToolSpec.ParamNames.output -> outputStore)
  }
  
  abstract class OneToOneTool(sig: UnarySig) extends Tool {
    
    override val spec: ToolSpec = ToolSpec.oneToOne(sig.input.spec, sig.output.spec)
  
    override val inputs: Map[LId, Store] = Map(ToolSpec.ParamNames.input -> sig.input)
    
    override val outputs: Map[LId, Store] = Map(ToolSpec.ParamNames.output -> sig.output)
  }
  
  abstract class NaryTool(sig: NarySig) extends Tool {
    
    def this(binarySig: BinarySig) = this(binarySig.toNarySig)
    
    override val spec: ToolSpec = ToolSpec(sig.inputs.map(_.toTuple).toMap, sig.outputs.map(_.toTuple).toMap)
  
    override val inputs: Map[LId, Store] = sig.inputs.map(i => i.id -> i).toMap   //TODO: correct?
    
    override val outputs: Map[LId, Store] = sig.outputs.map(o => o.id -> o).toMap //TODO: correct?
  }
  
  final case class CheckPreExistingVcfFile(vcfFile: Path) extends CheckPreexistingTool(vcfFile, FileStore.vcfFile(vcfFile))
  
  final case class CheckPreExistingPcaWeightsFile(pcaWeightsFile: Path) extends CheckPreexistingTool(pcaWeightsFile, FileStore.pcaWeightsFile(pcaWeightsFile))
  
  final case class ExtractSampleIdsFromVcfFile(
      vcfFile: Path, 
      sampleFile: Path) extends OneToOneTool(FileStore.vcfFile(vcfFile) ~> FileStore.sampleIdsFile(sampleFile)) {
    
    override val id = LId.LNamedId(s"Extracting sample ids from '$vcfFile' and storing them in '$sampleFile'")
  }
  
  final case class ConvertVcfToVds(vcfFile: Path, vdsDir: Path) extends OneToOneTool(FileStore.vcfFile(vcfFile) ~> FileStore.vdsFile(vdsDir)) {
    override val id = LId.LNamedId(s"Convert '$vcfFile' into Hail format VDS dir '$vdsDir'.")
  }

  final case class CalculateSingletons(vdsDir: Path, singletonsFile: Path) extends OneToOneTool(FileStore.vdsFile(vdsDir) ~> FileStore.singletonsFile(singletonsFile)) {
    override val id = LId.LNamedId(s"Calculate singletons from genotype calls in VDS format at '$vdsDir'.")
  }
  
  final case class ProjectPcaNative(vcfFile: Path, pcaWeightsFile: Path, klustaConfig: KlustaKwikKonfig) extends NaryTool((FileStore.vcfFile(vcfFile), FileStore.pcaWeightsFile(pcaWeightsFile)) ~> FileStore.pcaProjectedFile(klustaConfig.inputFile)) {
    override val id = LId.LNamedId(s"Project PCA using native method: vcf: '$vcfFile', pca weights: '$pcaWeightsFile'.")
  }
  
  final case class ProjectPca(vcfFile: Path, pcaWeightsFile: Path, klustaConfig: KlustaKwikKonfig) extends NaryTool((FileStore.vcfFile(vcfFile), FileStore.pcaWeightsFile(pcaWeightsFile)) ~> FileStore.pcaProjectedFile(klustaConfig.inputFile)) {
    override val id = LId.LNamedId(s"Project PCA: vcf: '$vcfFile', pca weights: '$pcaWeightsFile'.")
  }
  
  final case class KlustaKwikClustering(klustaConfig: KlustaKwikKonfig) extends OneToOneTool(FileStore.pcaProjectedFile(klustaConfig.inputFile) ~> FileStore.sampleClusterFile(klustaConfig.outputFile)) {
    override val id = LId.LNamedId(s"KlustaKwik Clustering: pca projected file '${klustaConfig.inputFile}', sample cluster file: '${klustaConfig.outputFile}'.")
  }
  
  final case class ClusteringSamplesByFeatures(klustaConfig: KlustaKwikKonfig) extends OneToOneTool(FileStore.pcaProjectedFile(klustaConfig.inputFile) ~> FileStore.sampleClusterFile(klustaConfig.outputFile)) {
    override val id = LId.LNamedId(s"Clustering Samples by Features: pca projected file '${klustaConfig.inputFile}', sample cluster file: '${klustaConfig.outputFile}'.")
  }

  /*def checkPreExistingVcfFile(id: String): Tool = nullaryTool(
      id, 
      "What a nice VCF file!",
      CoreStore.vcfFile,
      ToolSpec.preExistingCheckout(id))*/

  
  /*def checkPreExistingPcaWeightsFile(id: String): Tool = nullaryTool(
      id, 
      "File with PCA weights",
      CoreStore.pcaWeightsFile,
      ToolSpec.preExistingCheckout(id))*/

  
  /*val extractSampleIdsFromVcfFile: Tool = unaryTool(
      "Extracted sample ids from VCF file into a text file.",
      CoreStore.vcfFile ~> CoreStore.sampleIdsFile,
      ToolSpec.keyExtraction(ToolSpec.Indices.sampleKeyIndexInGenotypes) _)*/

  
  /*val importVcf: Tool = unaryTool(
      "Import VCF file into VDS format Hail works with.",
      CoreStore.vcfFile ~> CoreStore.vdsFile,
      ToolSpec.vcfImport(0) _)*/

  
  /*val calculateSingletons: Tool = unaryTool(
      "Calculate singletons from genotype calls in VDS format.",
      CoreStore.vdsFile ~> CoreStore.singletonsFile,
      ToolSpec.calculateSingletons(0) _)*/

  
  /*val projectPcaNative: Tool = binaryTool(
      "Project PCA using native method", 
      (CoreStore.vcfFile, CoreStore.pcaWeightsFile) ~> CoreStore.pcaProjectedFile)*/
      
  /*val projectPca: Tool = binaryTool(
      "Project PCA", 
       (CoreStore.vcfFile, CoreStore.pcaWeightsFile) ~> CoreStore.pcaProjectedFile)*/
  
  /*val klustaKwikClustering: Tool = unaryTool(
      "KlustaKwik Clustering", 
      CoreStore.pcaProjectedFile ~> CoreStore.sampleClusterFile)*/
  
  /*val clusteringSamplesByFeatures: Tool = unaryTool(
      "Clustering Samples by Features",
      CoreStore.pcaProjectedFile ~> CoreStore.sampleClusterFile)*/
      
  /*def tools(env: LEnv): Set[Tool] = {
    env.get(Keys.genotypesId).map(checkPreExistingVcfFile(_)).toSet ++
    env.get(Keys.pcaWeightsId).map(checkPreExistingPcaWeightsFile(_)).toSet ++
    Set(extractSampleIdsFromVcfFile, importVcf, calculateSingletons, projectPcaNative, projectPca, klustaKwikClustering)
  }*/
  
  //TODO: TEST
  def nullaryTool(id: String, name: String, output: Store, makeToolSpec: StoreSpec => ToolSpec): Tool = {
    CoreTool(
        name, 
        makeToolSpec(output.spec), 
        Map.empty[LId, Store], 
        Map(output.id -> output)) //TODO: correct?
  }
  
  //TODO: TEST
  def unaryTool(name: String, sig: UnarySig, makeToolSpec: (StoreSpec, StoreSpec) => ToolSpec): Tool = {
    CoreTool(
      name,
      makeToolSpec(sig.input.spec, sig.output.spec),
      Map(sig.input.id -> sig.input), //TODO: correct?
      Map(sig.output.id -> sig.output)) //TODO: correct?
  }
  
  //TODO: TEST
  def unaryTool(name: String, sig: UnarySig): Tool = nAryTool(name, sig.toNarySig)
  
  //TODO: TEST
  def binaryTool(name: String, sig: BinarySig): Tool = nAryTool(name, sig.toNarySig)
  
  //TODO: TEST
  def nAryTool(name: String, sig: NarySig): Tool = nAryTool(LId.LNamedId(name), sig)
  
  //TODO: TEST
  def nAryTool(sig: NarySig): Tool = nAryTool(LId.newAnonId, sig)
  
  //TODO: TEST
  def nAryTool(id: LId, sig: NarySig): Tool = {
    CoreTool(
      id,
      ToolSpec(sig.inputs.map(_.toTuple).toMap, sig.outputs.map(_.toTuple).toMap),
      sig.inputs.map(i => i.id -> i).toMap,  //TODO: correct?
      sig.outputs.map(o => o.id -> o).toMap) //TODO: correct?
  }
}
