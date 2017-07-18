package loamstream.loam

import java.nio.file.Paths

import loamstream.compiler.LoamCompiler
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.model.Tool
import loamstream.model.Store
import loamstream.model.execute.ExecutionEnvironment
import loamstream.compiler.LoamPredef
import loamstream.loam.ops.StoreType
import java.nio.file.Path
import loamstream.loam.files.LoamFileManager
import loamstream.util.Maps

/**
  * LoamStream
  * Created by oliverr on 6/13/2016.
  */
final class LoamGraphTest extends FunSuite {

  import LoamGraphTest._

  test("Test that valid graph passes all checks.") {
    val graph = makeTestComponents.graph
    
    val errors = LoamGraphValidation.allRules(graph)
    
    assert(errors.isEmpty, s"${errors.size} errors:\n${errors.map(_.message).mkString("\n")}")
  }
  
  test("Test rule eachStoreIsInputOrHasProducer") {
    val graph = makeTestComponents.graph
    
    val someStore = graph.storeProducers.keys.head
    val storeProducersWithoutSomeStore = graph.storeProducers - someStore
    val graphBroken = graph.copy(storeProducers = storeProducersWithoutSomeStore)

    assert(LoamGraphValidation.eachStoreIsInputOrHasProducer(graphBroken).nonEmpty)
  }
  
  test("Test rule eachStoresIsOutputOfItsProducer") {
    val graph = makeTestComponents.graph
    
    import Maps.Implicits._
    
    val toolOutputsShaved = graph.toolOutputs.strictMapValues(_.drop(1))
    
    val graphBroken = graph.copy(toolOutputs = toolOutputsShaved)
    assert(LoamGraphValidation.eachStoresIsOutputOfItsProducer(graphBroken).nonEmpty)
  }
  
  test("Test rule eachStoresIsInputOfItsConsumers") {
    val graph = makeTestComponents.graph
    
    import Maps.Implicits._
    
    val toolInputsShaved = graph.toolInputs.strictMapValues(_.drop(1))
    
    val graphBroken = graph.copy(toolInputs = toolInputsShaved)
    
    assert(LoamGraphValidation.eachStoresIsInputOfItsConsumers(graphBroken).nonEmpty)
  }
  
  test("Test rule eachToolsInputStoresArePresent") {
    val graph = makeTestComponents.graph
    
    val graphBroken = graph.copy(stores = Set.empty)
    
    assert(LoamGraphValidation.eachToolsInputStoresArePresent(graphBroken).nonEmpty)
  }
  
  test("Test rule eachToolsOutputStoresArePresent") {
    val graph = makeTestComponents.graph
    
    val graphBroken = graph.copy(stores = Set.empty)
    
    assert(LoamGraphValidation.eachToolsOutputStoresArePresent(graphBroken).nonEmpty)
  }
  
  test("Test rule eachStoreIsConnectedToATool") {
    val graph = makeTestComponents.graph
    
    val graphBroken = graph.copy(storeProducers = Map.empty, storeConsumers = Map.empty)
    
    assert(LoamGraphValidation.eachStoreIsConnectedToATool(graphBroken).nonEmpty)
  }
  
  test("Test rule eachToolHasEitherInputsOrOutputs") {
    val graph = makeTestComponents.graph
    
    val someTool = graph.tools.head
    val graphBroken = graph.copy(
        toolInputs = graph.toolInputs - someTool, 
        toolOutputs = graph.toolOutputs - someTool)
    
    assert(LoamGraphValidation.eachToolHasEitherInputsOrOutputs(graphBroken).nonEmpty)
  }
  
  test("Test rule allToolsAreConnected") {
    val graph = makeTestComponents.graph
    
    val someTool = graph.tools.head
    val graphBroken = graph.copy(
        toolInputs = graph.toolInputs - someTool, 
        toolOutputs = graph.toolOutputs - someTool)
    
    assert(LoamGraphValidation.allToolsAreConnected(graphBroken).nonEmpty)
  }
  
  test("Test rule graphIsAcyclic") {
    val graph = makeTestComponents.graph
    
    val someTool = graph.tools.head
    val toolInputsNew = graph.toolInputs + (someTool -> graph.stores)
    val toolOutputsNew = graph.toolOutputs + (someTool -> graph.stores)
    val graphBroken = graph.copy(toolInputs = toolInputsNew, toolOutputs = toolOutputsNew)
    
    assert(LoamGraphValidation.graphIsAcyclic(graphBroken).nonEmpty)
  }
  
  test("LoamGraph.pathOpt and LoamFileManager") {
    val components = makeTestComponents
    val graph = components.graph
    val fileManager = components.fileManager
    
    val pathInputFile = Paths.get("/user/home/someone/data.vcf")
    val pathOutputFile = Paths.get("/user/home/someone/dataImputed.vcf")
    val pathTemplate = Paths.get("/home/myself/template.vcf")
    
    val expectedStorePathOpts = Set(Some(pathInputFile), Some(pathTemplate), Some(pathOutputFile), None)
    
    assert(graph.stores.map(graph.pathOpt) === expectedStorePathOpts)
    
    for (store <- graph.stores) {
      val path = fileManager.getPath(store)
      val pathLastPart = path.getName(path.getNameCount - 1)
      assert(path === store.path)
      assert(pathLastPart.toString.startsWith(fileManager.filePrefix) || store.pathOpt.contains(path))
    }
  }
  
  test("without") {
    
    val components = makeTestComponents
    
    import components._
    import LoamGraph.StoreLocation.PathLocation
    import TestHelpers.path
    
    assert(graph.tools === Set(phaseTool, imputeTool))
    
    val filtered = graph.without(Set(imputeTool))
    
    assert(graph.tools === Set(phaseTool, imputeTool))
    
    assert(filtered.tools === Set(phaseTool))
    assert(filtered.stores === Set(raw, phased))
    assert(filtered.toolInputs === Map(phaseTool -> Set(raw)))
    assert(filtered.toolOutputs === Map(phaseTool -> Set(phased)))
    assert(filtered.inputStores === Set(raw))
    
    //NB: location of phased is not specified
    assert(filtered.storeLocations === Map(raw -> PathLocation(inputFile)))
    
    assert(filtered.storeProducers === Map(phased -> phaseTool))
    assert(filtered.storeConsumers === Map(raw -> Set(phaseTool)))
    
    assert(filtered.keysSameLists === graph.keysSameLists)
    assert(filtered.keysSameSets === graph.keysSameSets)
    
    assert(filtered.workDirs === Map(phaseTool -> TestHelpers.path(".")))
    
    assert(filtered.executionEnvironments === Map(phaseTool -> ExecutionEnvironment.Local))
  }
}

object LoamGraphTest {

  import StoreType._
  
  private def makeTestComponents: GraphComponents = {
    import TestHelpers.config
    
    implicit val scriptContext: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(config))
    
    import LoamPredef._
    import LoamCmdTool._
    
    val inputFile = path("/user/home/someone/data.vcf")
    val outputFile = path("/user/home/someone/dataImputed.vcf")
    
    val raw = store[VCF].at(inputFile).asInput
    val phased = store[VCF]
    val template = store[VCF].at(path("/home/myself/template.vcf")).asInput
    val imputed = store[VCF].at(outputFile)
    
    val phaseTool = cmd"shapeit -in $raw -out $phased"
    val imputeTool = cmd"impute -in $phased -template $template -out $imputed".using("R-3.1")
    
    GraphComponents(
      graph = scriptContext.projectContext.graph,
      fileManager = scriptContext.projectContext.fileManager,
      inputFile = inputFile,
      outputFile = outputFile,
      raw = raw,
      phased = phased,
      template = template,
      imputed = imputed,
      phaseTool = phaseTool,
      imputeTool = imputeTool)
  }
  
  private final case class GraphComponents(
    graph: LoamGraph,
    fileManager: LoamFileManager,
    inputFile: Path,
    outputFile: Path,
    raw: Store[VCF],
    phased: Store[VCF],
    template: Store[VCF],
    imputed: Store[VCF],
    phaseTool: LoamCmdTool,
    imputeTool: LoamCmdTool)
}
