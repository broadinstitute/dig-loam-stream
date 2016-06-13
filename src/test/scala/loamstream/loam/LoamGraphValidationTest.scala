package loamstream.loam

import loamstream.compiler.ClientMessageHandler.OutMessageSink
import loamstream.compiler.{LoamCompiler, LoamRepository}
import loamstream.loam.LoamGraph.StoreSource
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * LoamStream
  * Created by oliverr on 6/13/2016.
  */
class LoamGraphValidationTest extends FunSuite {

  val graph = {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)
    val codeShot = LoamRepository.defaultRepo.get("impute.loam")
    val result = compiler.compile(codeShot.get)
    result.graphOpt.get
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
      graph.storeSources.mapValues(source => StoreSource.FromTool(someTool)).view.force
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
    val graphBroken = graph.copy(storeSources = Map.empty, storeConsumers = Map.empty)
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

}
