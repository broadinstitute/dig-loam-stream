package loamstream.loam

import java.io.File

import loamstream.loam.LoamToken.{StoreToken, StringToken}
import loamstream.loam.ops.StoreType.{TXT, VCF}
import loamstream.model.{LId, Store}
import loamstream.util.BashScript
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.ops.StoreType
import loamstream.conf.LoamConfig
import loamstream.conf.ExecutionConfig
import loamstream.loam.LoamToken.StoreRefToken
import com.typesafe.config.ConfigFactory
import loamstream.conf.DynamicConfig
import loamstream.compiler.LoamPredef._

/**
  * @author clint
  *         date: Jul 21, 2016
  */
final class LoamCmdToolTest extends FunSuite {
  //scalastyle:off magic.number
  
  import LoamCmdTool._
  import TestHelpers.config

  private def emptyProjectContext = LoamProjectContext.empty(config)

  private val fileSepForBash = BashScript.escapeString(File.separator)
  
  test("string interpolation (trivial)") {
    implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

    val tool = cmd"foo bar baz"

    assert(tool.graph eq scriptContext.projectContext.graph)

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

  test("using() in any order with in() and out()") {
    implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

    val input1 = 42
    val input2 = "input2"
    val input3 = store[TXT].at("/inputStore").asInput
    val output = store[VCF].at("/outputStore")

    val useuse = "source /broad/software/scripts/useuse"
    val expectedCmdLineString =
      s"$useuse && reuse -q R-3.1 && " +
      s"(someTool --in 42 --in input2 --in ${fileSepForBash}inputStore --out ${fileSepForBash}outputStore)"

    val baseTool = cmd"someTool --in $input1 --in $input2 --in $input3 --out $output"

    val toolv1 = baseTool.in(input3).out(output).using("R-3.1")

    assert(toolv1.graph eq scriptContext.projectContext.graph)
    assert(toolv1.graph.stores.size === 2)
    assert(toolv1.graph.storeProducers.size === 1)
    assert(toolv1.graph.storeConsumers.size === 2)
    assert(toolv1.graph.toolInputs.size === 1)
    assert(toolv1.graph.toolOutputs.size === 1)
    assert(toolv1.graph.tools === Set(toolv1))
    assert(toolv1.inputs.size === 1)
    assert(toolv1.outputs.size === 1)

    assert(toolv1.commandLine === expectedCmdLineString)

    val toolv2 = baseTool.in(input3).using("R-3.1").out(output)

    assert(toolv2.commandLine === expectedCmdLineString)

    val toolv3 = baseTool.using("R-3.1").in(input3).out(output)

    assert(toolv2.commandLine === expectedCmdLineString)
  }

  test("using() in a more complex cmd") {
    implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

    val input = store[TXT].at("/inputStore").asInput
    val output = store[VCF].at("/outputStore")
    val someOtherTool = "someOtherTool"

    val tool = cmd"(echo 10 ; sed '1d' $input | cut -f5- | sed 's/\t/ /g') > $output".using(someOtherTool)

    val useuse = "source /broad/software/scripts/useuse"
    val expected = s"$useuse && reuse -q someOtherTool && " +
      s"((echo 10 ; sed '1d' ${fileSepForBash}inputStore | cut -f5- | sed 's/\\t/ /g') > ${fileSepForBash}outputStore)"

    assert(tool.commandLine === expected)
  }

  test("using() with multiple tools to be 'use'd") {
    implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

    val tool = cmd"someTool".using("otherTool1", "otherTool2", "otherTool3")

    val useuse = "source /broad/software/scripts/useuse"
    val expected = s"$useuse && reuse -q otherTool1 && reuse -q otherTool2 && reuse -q otherTool3 && (someTool)"

    assert(tool.commandLine === expected)
  }

  private def storeMap(stores: Iterable[Store.Untyped]): Map[LId, Store.Untyped] = {
    stores.map(store => store.id -> store).toMap
  }

  private def tokens(values: AnyRef*): Seq[LoamToken] = values.map {
    case string: String => StringToken(string)
    case store: Store.Untyped => StoreToken(store)
  }

  private def assertGraph(graph: LoamGraph, tool: LoamCmdTool, expectedTokens: Seq[LoamToken],
                  expectedInputs: Map[LId, Store.Untyped],
                  expectedOutputs: Map[LId, Store.Untyped]): Unit = {
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
                           inputsBefore: Set[Store.Untyped], outputsBefore: Set[Store.Untyped],
                           stores: Seq[Store.Untyped], addInputsFirst: Boolean = true): Unit = {
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

  test("toToken") {

    //Needed to allow making stores
    implicit def newScriptContext: LoamScriptContext = {
      val loamConfig = LoamConfig(None, None, None, None, None, ExecutionConfig.default)
      
      val projectContext = LoamProjectContext.empty(loamConfig)
    
      new LoamScriptContext(projectContext)
    }
    
    import LoamCmdTool.toToken
    
    //Store
    val store = Store[StoreType.TXT](LId.newAnonId)
    
    assert(toToken(store) === StoreToken(store))
    
    //StoreRef:
    val storeRef = LoamStoreRef(store, identity)
    
    assert(toToken(storeRef) === StoreRefToken(storeRef))
    
    //DynamicConfig
    
    val config = DynamicConfig(ConfigFactory.parseString("foo { bar { baz = 42 } }"), Some("foo.bar.baz"))
    
    assert(toToken(config) === StringToken("42"))
    
    val configThatShouldBlowUp = config.copy(pathOption = Some("blerg.zerg"))
    
    intercept[Exception] {
      toToken(configThatShouldBlowUp)
    }
    
    //arbitrary type
    final case class Foo(x: Int)
    
    assert(toToken(Foo(42)) === StringToken("Foo(42)"))
  }
  
  test("in() and out() with no implicit i/o stores") {
    for (addInputsFirst <- Seq(true, false)) {
      implicit val projectContext = emptyProjectContext
      implicit val scriptContext = new LoamScriptContext(projectContext)

      val nStores = 4
      val stores = Seq.fill[Store.Untyped](nStores)(Store[TXT](LId.newAnonId))

      val tool = cmd"foo bar baz"

      val expectedTokens = tokens("foo bar baz")
      val inputsBefore = Set.empty[Store.Untyped]
      val outputsBefore = Set.empty[Store.Untyped]
      assertAddingIOStores(projectContext, tool, expectedTokens, inputsBefore, outputsBefore, stores, addInputsFirst)
    }
  }

  test("in() and out() mixed with implicit i/o stores") {
    for (addInputsFirst <- Seq(true, false)) {
      implicit val projectContext = emptyProjectContext
      implicit val scriptContext = new LoamScriptContext(projectContext)

      val nStores = 6
      val stores = Seq.fill[Store.Untyped](nStores)(Store[TXT](LId.newAnonId))

      val inStoreImplicit = stores(4).at("inputFile.vcf").asInput // scalastyle:ignore magic.number
      val outStoreImplicit = stores(5).at("outputFile.txt") // scalastyle:ignore magic.number

      val tool = cmd"foo $inStoreImplicit $outStoreImplicit"

      val expectedTokens = tokens("foo ", inStoreImplicit, " ", outStoreImplicit)
      val inputsBefore = Set[Store.Untyped](inStoreImplicit)
      val outputsBefore = Set[Store.Untyped](outStoreImplicit)
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
  
  //scalastyle:on magic.number
}
