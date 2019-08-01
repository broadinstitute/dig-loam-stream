package loamstream.loam

import java.nio.file.Path
import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.Store
import loamstream.util.Maps
import loamstream.model.execute.LocalSettings

/**
  * LoamStream
  * Created by oliverr on 6/13/2016.
  */
final class LoamGraphTest extends FunSuite {

  import LoamGraphTest._

  test("updateTool") {
    val components = makeTestComponents
    
    val oldGraph = components.graph.withToolName(components.imputeTool, "impute")
    
    val oldTool = components.imputeTool
    val newTool = oldTool.copy(
        id = oldTool.id, 
        tokens = (LoamToken.StringToken("NEW ") +: oldTool.tokens))(oldTool.scriptContext)
    
    val newGraph = oldGraph.updateTool(oldTool, newTool)
    
    assert(oldGraph !== newGraph)
    
    assert(oldGraph.tools === components.tools)
    assert(newGraph.tools === Set(components.phaseTool, newTool))
    
    assert(oldGraph.toolInputs === Map(
        components.phaseTool -> Set(components.raw),
        components.imputeTool -> Set(components.phased, components.template)))
    
    assert(newGraph.toolInputs === Map(
        components.phaseTool -> Set(components.raw),
        newTool -> Set(components.phased, components.template)))
        
    assert(oldGraph.toolOutputs === Map(
        components.phaseTool -> Set(components.phased),
        components.imputeTool -> Set(components.imputed)))
        
    assert(newGraph.toolOutputs === Map(
        components.phaseTool -> Set(components.phased),
        newTool -> Set(components.imputed)))
        
    assert(oldGraph.storeProducers.toMap === Map(
        components.phased -> components.phaseTool,
        components.imputed -> components.imputeTool))
        
    assert(newGraph.storeProducers.toMap === Map(
        components.phased -> components.phaseTool,
        components.imputed -> newTool))
        
    assert(oldGraph.storeConsumers === Map(
        components.raw -> Set(components.phaseTool),
        components.phased -> Set(components.imputeTool),
        components.template -> Set(components.imputeTool)))
        
    assert(newGraph.storeConsumers === Map(
        components.raw -> Set(components.phaseTool),
        components.phased -> Set(newTool),
        components.template -> Set(newTool)))
        
    import TestHelpers.path
        
    assert(oldGraph.workDirs === Map(components.phaseTool -> path("."), components.imputeTool -> path(".")))
    
    assert(newGraph.workDirs === Map(components.phaseTool -> path("."), newTool -> path(".")))
    
    assert(oldGraph.toolSettings === Map(
        components.phaseTool -> LocalSettings, components.imputeTool -> LocalSettings))
        
    assert(newGraph.toolSettings === Map(
        components.phaseTool -> LocalSettings, newTool -> LocalSettings))
    
    assert(oldGraph.nameOf(components.phaseTool) === Some("phase"))
    assert(oldGraph.nameOf(components.imputeTool) === Some("impute"))
    
    assert(newGraph.nameOf(components.phaseTool) === Some("phase"))
    assert(newGraph.nameOf(newTool) === Some("impute"))
    
    assert(oldGraph.stores === newGraph.stores)
    assert(oldGraph.inputStores === newGraph.inputStores)
  }

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
    
    val pathInputFile = Paths.get("/user/home/someone/data.vcf")
    val pathOutputFile = Paths.get("/user/home/someone/dataImputed.vcf")
    val pathTemplate = Paths.get("/home/myself/template.vcf")
    
    val expectedStorePathOpts = Set(
        Some(pathInputFile), Some(pathTemplate), Some(pathOutputFile), Some(components.phased.path))
    
    assert(graph.stores.map(_.pathOpt) === expectedStorePathOpts)
    
    for (store <- graph.stores) {
      val path = store.path
      val pathLastPart = path.getName(path.getNameCount - 1)
      assert(path === store.path)
      assert(pathLastPart.toString.startsWith("loam") || store.pathOpt.contains(path))
    }
  }
  
  test("without") {
    
    val components = makeTestComponents
    
    import components._
    import LoamGraph.StoreLocation.PathLocation
    import TestHelpers.path
    
    assert(graph.tools === Set(phaseTool, imputeTool))
    
    assert(graph.nameOf(phaseTool) === Some("phase"))
    assert(graph.nameOf(imputeTool).isDefined)
    
    val filtered = graph.without(Set(imputeTool))
    
    assert(graph.tools === Set(phaseTool, imputeTool))
    
    assert(filtered.tools === Set(phaseTool))
    assert(filtered.stores === Set(raw, phased))
    assert(filtered.toolInputs === Map(phaseTool -> Set(raw)))
    assert(filtered.toolOutputs === Map(phaseTool -> Set(phased)))
    assert(filtered.inputStores === Set(raw))
    
    assert((graph.stores - imputed - template) === filtered.stores)
    
    assert(filtered.storeProducers.toMap === Map(phased -> phaseTool))
    assert(filtered.storeConsumers === Map(raw -> Set(phaseTool)))
    
    assert(filtered.workDirs === Map(phaseTool -> TestHelpers.path(".")))
    
    assert(filtered.toolSettings === Map(phaseTool -> LocalSettings))
    
    assert(filtered.namedTools.toMap === Map(phaseTool -> "phase"))
  }
  
  test("withToolName") {
    val components = makeTestComponents
    
    import components._
    import LoamGraph.StoreLocation.PathLocation
    import TestHelpers.path
    
    assert(graph.tools === Set(phaseTool, imputeTool))
    
    assert(graph.nameOf(phaseTool) === Some("phase"))
    assert(graph.nameOf(imputeTool).isDefined)
    
    //renaming a tool
    intercept[Exception] {
      graph.withToolName(phaseTool, "asdf")
    }
    
    //non-unique name
    intercept[Exception] {
      graph.withToolName(imputeTool, "phase")
    }
    
    //Give a tool with an auto-generated name a user-specified name
    val updatedGraph = graph.withToolName(imputeTool, "impute")
    
    assert(updatedGraph.namedTools.toMap === Map(phaseTool -> "phase", imputeTool -> "impute"))
  }
}

object LoamGraphTest {

  private def makeTestComponents: GraphComponents = {
    import loamstream.TestHelpers.config
    
    implicit val scriptContext: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(config))
    
    import loamstream.loam.LoamSyntax._
    
    val inputFile = path("/user/home/someone/data.vcf")
    val outputFile = path("/user/home/someone/dataImputed.vcf")
    
    val raw = store(inputFile).asInput
    val phased = store
    val template = store(path("/home/myself/template.vcf")).asInput
    val imputed = store(outputFile)
    
    val phaseTool = cmd"shapeit -in $raw -out $phased".tag("phase")
    val imputeTool = cmd"impute -in $phased -template $template -out $imputed".using("R-3.1")
    
    GraphComponents(
      graph = scriptContext.projectContext.graph,
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
      inputFile: Path,
      outputFile: Path,
      raw: Store,
      phased: Store,
      template: Store,
      imputed: Store,
      phaseTool: LoamCmdTool,
      imputeTool: LoamCmdTool) {
    
    def tools: Set[LoamCmdTool] = Set(phaseTool, imputeTool)
  }
}
