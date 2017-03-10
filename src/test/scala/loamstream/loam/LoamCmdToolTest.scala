package loamstream.loam

import loamstream.loam.LoamToken.{StoreToken, StringToken}
import loamstream.loam.ops.StoreType.TXT
import loamstream.model.LId
import loamstream.util.BashScript
import org.scalatest.FunSuite
import loamstream.TestHelpers

/**
  * @author clint
  *         date: Jul 21, 2016
  */
final class LoamCmdToolTest extends FunSuite {

  import LoamCmdTool._
  import TestHelpers.config

  private def emptyProjectContext = LoamProjectContext.empty(config)
  
  test("string interpolation (trivial)") {
    implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

    val tool = cmd"foo bar baz"

    assert(tool.graph eq scriptContext.projectContext.graphBox.value)

    assert(tool.graph.stores == Set.empty)
    assert(tool.graph.storeProducers === Map.empty)
    assert(tool.graph.storeConsumers == Map.empty)

    assert(tool.graph.toolInputs == Map(tool -> Set.empty))
    assert(tool.graph.toolOutputs == Map(tool -> Set.empty))

    assert(tool.graph.tools == Set(tool))

    assert(tool.inputs == Map.empty)
    assert(tool.outputs == Map.empty)
    assert(tool.tokens == Seq(StringToken("foo bar baz")))
  }

  private def storeMap(stores: Iterable[LoamStore.Untyped]): Map[LId, LoamStore.Untyped] = {
    stores.map(store => store.id -> store).toMap
  }

  private def tokens(values: AnyRef*): Seq[LoamToken] = values.map {
    case string: String => StringToken(string)
    case store: LoamStore.Untyped => StoreToken(store)
  }

  private def assertGraph(graph: LoamGraph, tool: LoamCmdTool, expectedTokens: Seq[LoamToken],
                  expectedInputs: Map[LId, LoamStore.Untyped],
                  expectedOutputs: Map[LId, LoamStore.Untyped]): Unit = {
    assert(tool.tokens === expectedTokens)
    assert(tool.inputs === expectedInputs)
    assert(tool.outputs === expectedOutputs)
    
    for ((id, store) <- expectedInputs) {
      val storeProducerOpt = graph.storeProducers.get(store)
      assert(storeProducerOpt === None,
        s"Expected no producer for input store $id, but got tool ${storeProducerOpt.map(_.id).getOrElse("")}")
      val storeConsumers = graph.storeConsumers.getOrElse(store, Set.empty)
      assert(storeConsumers === Set(tool), {
        val storeConsumersString = if (storeConsumers.isEmpty) {
          "none"
        } else {
          storeConsumers.map(_.id).mkString(", ")
        }
        s"Expected tool ${tool.id} as consumer of input store $id, but got $storeConsumersString."
      })
    }
    
    for ((id, store) <- expectedOutputs) {
      val storeProducerOpt = graph.storeProducers.get(store)
      assert(storeProducerOpt === Some(tool), {
        val storeProducerString = storeProducerOpt.map(_.id.toString).getOrElse("none")
        s"Expected tool ${tool.id} as producer of output store $id, but got $storeProducerString."
      })
      val storeConsumers = graph.storeConsumers.getOrElse(store, Set.empty)
      assert(storeConsumers === Set.empty,
        s"Expected no consumers of output store $id, but got tools ${storeConsumers.map(_.id).mkString(", ")}")
    }
  }

  private def assertAddingIOStores(context: LoamProjectContext, tool: LoamCmdTool, expectedTokens: Seq[LoamToken],
                           inputsBefore: Set[LoamStore.Untyped], outputsBefore: Set[LoamStore.Untyped],
                           stores: Seq[LoamStore.Untyped], addInputsFirst: Boolean = true): Unit = {
    val inputsMapBefore = storeMap(inputsBefore)
    val outputsMapBefore = storeMap(outputsBefore)
    val inputsMapAfter = storeMap(inputsBefore + stores.head + stores(2))
    val outputsMapAfter = storeMap(outputsBefore + stores(1) + stores(3))

    assertGraph(context.graph, tool, expectedTokens, inputsMapBefore, outputsMapBefore)
    if (addInputsFirst) {
      tool.in(stores.head, stores(2))
      assertGraph(context.graph, tool, expectedTokens, inputsMapAfter, outputsMapBefore)
      tool.out(stores(1), stores(3))
    } else {
      tool.out(stores(1), stores(3))
      assertGraph(context.graph, tool, expectedTokens, inputsMapBefore, outputsMapAfter)
      tool.in(stores.head, stores(2))
    }
    assertGraph(context.graph, tool, expectedTokens, inputsMapAfter, outputsMapAfter)
  }

  test("in() and out() with no implicit i/o stores") {
    for (addInputsFirst <- Seq(true, false)) {
      implicit val projectContext = emptyProjectContext
      implicit val scriptContext = new LoamScriptContext(projectContext)

      val nStores = 4
      val stores = Seq.fill[LoamStore.Untyped](nStores)(LoamStore[TXT](LId.newAnonId))

      val tool = cmd"foo bar baz"

      val expectedTokens = tokens("foo bar baz")
      val inputsBefore = Set.empty[LoamStore.Untyped]
      val outputsBefore = Set.empty[LoamStore.Untyped]
      assertAddingIOStores(projectContext, tool, expectedTokens, inputsBefore, outputsBefore, stores, addInputsFirst)
    }
  }

  test("in() and out() mixed with implicit i/o stores") {
    for (addInputsFirst <- Seq(true, false)) {
      implicit val projectContext = emptyProjectContext
      implicit val scriptContext = new LoamScriptContext(projectContext)

      val nStores = 6
      val stores = Seq.fill[LoamStore.Untyped](nStores)(LoamStore[TXT](LId.newAnonId))

      val inStoreImplicit = stores(4).at("inputFile.vcf").asInput // scalastyle:ignore magic.number
      val outStoreImplicit = stores(5).at("outputFile.txt") // scalastyle:ignore magic.number

      val tool = cmd"foo $inStoreImplicit $outStoreImplicit"

      val expectedTokens = tokens("foo ", inStoreImplicit, " ", outStoreImplicit)
      val inputsBefore = Set[LoamStore.Untyped](inStoreImplicit)
      val outputsBefore = Set[LoamStore.Untyped](outStoreImplicit)
      assertAddingIOStores(projectContext, tool, expectedTokens, inputsBefore, outputsBefore, stores, addInputsFirst)
    }
  }

  test("at(...) and asInput") {
    implicit val scriptContext = new LoamScriptContext(emptyProjectContext)
    import loamstream.compiler.LoamPredef._
    val inStoreWithPath = store[TXT].at("dir/inStoreWithPath.txt").asInput
    val outStoreWithPath = store[TXT].at("dir/outStoreWithPath.txt")
    val inStoreWithUri = store[TXT].at(uri("xyz://host/dir/inStoreWithUri")).asInput
    val outStoreWithUri = store[TXT].at(uri("xyz://host/dir/outStoreWithUri"))
    val tool = cmd"maker $inStoreWithPath $inStoreWithUri $outStoreWithPath $outStoreWithUri"
    val inPath = BashScript.escapeString(inStoreWithPath.path.toString)
    val outPath = BashScript.escapeString(outStoreWithPath.path.toString)
    val inUri = BashScript.escapeString(inStoreWithUri.uriOpt.get.toString)
    val outUri = BashScript.escapeString(outStoreWithUri.uriOpt.get.toString)
    val commandLineExpected = s"maker $inPath $inUri $outPath $outUri"
    assert(tool.commandLine === commandLineExpected)
  }
}
