package loamstream.loam

import java.nio.file.Paths

import loamstream.compiler.LoamCompiler
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.model.Tool
import loamstream.model.Store
import loamstream.loam.LoamGraph.StoreLocation
import loamstream.model.execute.ExecutionEnvironment

/**
  * LoamStream
  * Created by oliverr on 6/13/2016.
  */
final class LoamGraphTest extends FunSuite {

  private val phaseCommand = "shapeit"
  private val imputeCommand = "impute2"
  
  private val code = {
    s"""
      |val inputFile = path("/user/home/someone/data.vcf")
      |val outputFile = path("/user/home/someone/dataImputed.vcf")
      |
      |val raw = store[VCF].at(inputFile).asInput
      |val phased = store[VCF]
      |val template = store[VCF].at(path("/home/myself/template.vcf")).asInput
      |val imputed = store[VCF].at(outputFile)
      |
      |cmd"$phaseCommand -in $$raw -out $$phased"
      |cmd"$imputeCommand -in $$phased -template $$template -out $$imputed".using("R-3.1")
      | """.stripMargin
  }

  private val context = {
    val compiler = new LoamCompiler

    val result = compiler.compile(TestHelpers.config, LoamScript("LoamGraphTestScript1", code))

    result.contextOpt.get
  }
  
  private val graph = context.graph
  
  private lazy val (phaseTool, imputeTool) = {
    def commandLine(t: Tool) = t.asInstanceOf[LoamCmdTool].commandLine
    
    val phaseCmd = graph.tools.collect { case t if commandLine(t).trim.startsWith(phaseCommand) => t }.head
    val imputeCmd = graph.tools.collect { case t if commandLine(t).contains(imputeCommand) => t }.head
    
    (phaseCmd, imputeCmd)
  }

  test("Test that valid graph passes all checks.") {
    val errors = LoamGraphValidation.allRules(graph)
    assert(errors.isEmpty, s"${errors.size} errors:\n${errors.map(_.message).mkString("\n")}")
  }
  
  test("Test rule eachStoreIsInputOrHasProducer") {
    val someStore = graph.storeProducers.head._1
    val storeProducersWithoutSomeStore = graph.storeProducers - someStore
    val graphBroken = graph.copy(storeProducers = storeProducersWithoutSomeStore)
    assert(LoamGraphValidation.eachStoreIsInputOrHasProducer(graphBroken).nonEmpty)
  }
  
  test("Test rule eachStoresIsOutputOfItsProducer") {
    val toolOutputsShaved =
      graph.toolOutputs.mapValues(outputs => if (outputs.nonEmpty) outputs - outputs.head else outputs).view.force
    val graphBroken = graph.copy(toolOutputs = toolOutputsShaved)
    assert(LoamGraphValidation.eachStoresIsOutputOfItsProducer(graphBroken).nonEmpty)
  }
  
  test("Test rule eachStoresIsInputOfItsConsumers") {
    val toolInputsShaved =
      graph.toolInputs.mapValues(inputs => if (inputs.nonEmpty) inputs - inputs.head else inputs).view.force
    val graphBroken = graph.copy(toolInputs = toolInputsShaved)
    assert(LoamGraphValidation.eachStoresIsInputOfItsConsumers(graphBroken).nonEmpty)
  }
  
  test("Test rule eachToolsInputStoresArePresent") {
    val graphBroken = graph.copy(stores = Set.empty)
    assert(LoamGraphValidation.eachToolsInputStoresArePresent(graphBroken).nonEmpty)
  }
  
  test("Test rule eachToolsOutputStoresArePresent") {
    val graphBroken = graph.copy(stores = Set.empty)
    assert(LoamGraphValidation.eachToolsOutputStoresArePresent(graphBroken).nonEmpty)
  }
  
  test("Test rule eachStoreIsConnectedToATool") {
    val graphBroken = graph.copy(storeProducers = Map.empty, storeConsumers = Map.empty)
    assert(LoamGraphValidation.eachStoreIsConnectedToATool(graphBroken).nonEmpty)
  }
  
  test("Test rule eachToolHasEitherInputsOrOutputs") {
    val someTool = graph.tools.head
    val graphBroken = graph.copy(toolInputs = graph.toolInputs - someTool, toolOutputs = graph.toolOutputs - someTool)
    assert(LoamGraphValidation.eachToolHasEitherInputsOrOutputs(graphBroken).nonEmpty)
  }
  
  test("Test rule allToolsAreConnected") {
    val someTool = graph.tools.head
    val graphBroken = graph.copy(toolInputs = graph.toolInputs - someTool, toolOutputs = graph.toolOutputs - someTool)
    assert(LoamGraphValidation.allToolsAreConnected(graphBroken).nonEmpty)
  }
  
  test("Test rule graphIsAcyclic") {
    val someTool = graph.tools.head
    val toolInputsNew = graph.toolInputs + (someTool -> graph.stores)
    val toolOutputsNew = graph.toolOutputs + (someTool -> graph.stores)
    val graphBroken = graph.copy(toolInputs = toolInputsNew, toolOutputs = toolOutputsNew)
    assert(LoamGraphValidation.graphIsAcyclic(graphBroken).nonEmpty)
  }
  
  test("LoamGraph.pathOpt and LoamFileManager") {
    val pathInputFile = Paths.get("/user/home/someone/data.vcf")
    val pathOutputFile = Paths.get("/user/home/someone/dataImputed.vcf")
    val pathTemplate = Paths.get("/home/myself/template.vcf")
    assert(graph.stores.map(graph.pathOpt) ===
      Set(Some(pathInputFile), Some(pathTemplate), Some(pathOutputFile), None))
    val fileManager = context.fileManager
    for (store <- graph.stores) {
      val path = fileManager.getPath(store)
      val pathLastPart = path.getName(path.getNameCount - 1)
      assert(path === store.path)
      assert(pathLastPart.toString.startsWith(fileManager.filePrefix) || store.pathOpt.contains(path))
    }
  }
  
  test("without") {
    
    def findStore(fragment: String)(matches: String => String => Boolean): Store.Untyped = {
      graph.stores.collect { case s if matches(s.path.toString)(fragment) => s }.head
    }
    
    def findStoreBySuffix(fileName: String): Store.Untyped = findStore(fileName)(_.endsWith)
    
    def findStoreByPrefix(prefix: String): Store.Untyped = findStore(prefix)(_.startsWith)
    
    val raw = findStoreBySuffix("data.vcf")
    val phased = findStoreByPrefix("/tmp")
    val template = findStoreBySuffix("template.vcf")
    val imputed = findStoreBySuffix("dataImputed.vcf")
    
    import TestHelpers.path
    import LoamGraph.StoreLocation.PathLocation
    
    assert(graph.tools === Set(phaseTool, imputeTool))
    
    val filtered = graph.without(Set(imputeTool))
    
    assert(graph.tools === Set(phaseTool, imputeTool))
    
    assert(filtered.tools === Set(phaseTool))
    assert(filtered.stores === Set(raw, phased))
    assert(filtered.toolInputs === Map(phaseTool -> Set(raw)))
    assert(filtered.toolOutputs === Map(phaseTool -> Set(phased)))
    assert(filtered.inputStores === Set(raw))
    
    //NB: location of phased is not specified
    assert(filtered.storeLocations === Map(raw -> PathLocation(path("/user/home/someone/data.vcf"))))
    
    assert(filtered.storeProducers === Map(phased -> phaseTool))
    assert(filtered.storeConsumers === Map(raw -> Set(phaseTool)))
    
    assert(filtered.keysSameLists === graph.keysSameLists)
    assert(filtered.keysSameSets === graph.keysSameSets)
    
    assert(filtered.workDirs === Map(phaseTool -> path(".")))
    
    assert(filtered.executionEnvironments === Map(phaseTool -> ExecutionEnvironment.Local))
  }
}
