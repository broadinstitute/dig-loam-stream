package loamstream.loam

import java.nio.file.Paths

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.files.LoamFileManager
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 6/13/2016.
  */
final class LoamGraphTest extends FunSuite {

  val code =
    """
      |val inputFile = path("/user/home/someone/data.vcf")
      |val outputFile = path("/user/home/someone/dataImputed.vcf")
      |val phaseCommand = "shapeit"
      |val imputeCommand = "impute2"
      |
      |val raw = store[VCF].from(inputFile)
      |val phased = store[VCF]
      |val template = store[VCF].from(path("/home/myself/template.vcf"))
      |val imputed = store[VCF].to(outputFile)
      |
      |cmd"$phaseCommand -in $raw -out $phased"
      |cmd"$imputeCommand -in $phased -template $template -out $imputed"
      | """.stripMargin

  implicit val context = {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)

    val result = compiler.compile(code)
    
    result.contextOpt.get
  }
  val graph = context.graph
  test("Test that valid graph passes all checks.") {
    assert(LoamGraphValidation.allRules(graph).isEmpty)
  }
  test("Test rule eachStoreHasASource") {
    val someStore = graph.storeSources.head._1
    val storeSourcesWithoutSomeStore = graph.storeSources - someStore
    val graphBroken = graph.copy(storeSources = storeSourcesWithoutSomeStore)
    assert(LoamGraphValidation.eachStoreHasASource(graphBroken).nonEmpty)
  }
  test("Test rule eachToolSourcedStoreIsOutputOfThatTool") {
    val someTool = graph.tools.head
    val storeSourcesAllFromSameTool =
      graph.storeSources.mapValues(source => StoreEdge.ToolEdge(someTool)).view.force
    val graphBroken = graph.copy(storeSources = storeSourcesAllFromSameTool)
    assert(LoamGraphValidation.eachToolSourcedStoreIsOutputOfThatTool(graphBroken).nonEmpty)
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
    val graphBroken = graph.copy(storeSources = Map.empty, storeSinks = Map.empty)
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
}
