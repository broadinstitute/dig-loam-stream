package loamstream.loam

import java.io.File
import java.nio.file.Paths

import org.scalatest.FunSuite

import com.typesafe.config.ConfigFactory

import loamstream.compiler.LoamPredef._
import loamstream.conf.DynamicConfig
import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.StringToken
import loamstream.model.LId
import loamstream.model.Store
import loamstream.util.BashScript
import java.nio.file.Path
import loamstream.TestHelpers
import loamstream.LoamFunSuite
import loamstream.model.Tool
import loamstream.compiler.LoamPredef

/**
  * @author clint
  *         date: Jul 21, 2016
  */
final class LoamCmdToolTest extends LoamFunSuite {
  import loamstream.loam.LoamSyntax._
  import loamstream.TestHelpers.emptyProjectContext
 
  // Since all the cmd"..." are placed into a script file to be executed
  // with `sh`, the path separator should remain '/' as opposed to the
  // platform separator.
  private val fileSepForBash = '/'
  
  private def nameOf(t: Tool) = t.graph.nameOf(t)
  
  testWithScriptContext("string interpolation (trivial)") { implicit scriptContext =>
    val tool = cmd"foo bar baz"

    assert(tool.graph eq scriptContext.projectContext.graph)

    assert(nameOf(tool).isDefined)
    
    assert(tool.graph.stores == Set.empty)
    assert(tool.graph.storeProducers.toMap === Map.empty)
    assert(tool.graph.storeConsumers == Map.empty)

    assert(tool.graph.toolInputs == Map(tool -> Set.empty))
    assert(tool.graph.toolOutputs == Map(tool -> Set.empty))

    assert(tool.graph.tools == Set(tool))

    assert(tool.inputs == Map.empty)
    assert(tool.outputs == Map.empty)
    assert(tool.tokens == Seq(StringToken("foo bar baz")))
  }
  
  testWithScriptContext("string interpolation (more complex)") { implicit scriptContext =>
    val is = Seq(3,4,5)
    val nuh = store("nuh.txt")
    val zuh = store("zuh.txt.bar")
    val stores = Seq(nuh, zuh)
    
    // properly escaped for the bash scripts
    val nuhPath = nuh.render
    val zuhPath = zuh.render

    val tool = cmd"foo bar --is $is baz --in $nuh --blarg $stores"
    
    assert(tool.graph eq scriptContext.projectContext.graph)

    assert(tool.graph.stores === Set(nuh, zuh))
    assert(nameOf(tool).isDefined)
    
    assert(tool.graph.storeProducers.toMap === Map(nuh -> tool))
    assert(tool.graph.storeConsumers == Map.empty)

    assert(tool.graph.toolInputs == Map(tool -> Set.empty))
    assert(tool.graph.toolOutputs == Map(tool -> Set(nuh)))

    assert(tool.graph.tools == Set(tool))

    assert(tool.inputs == Map.empty)
    assert(tool.outputs == Map(nuh.id -> nuh))
    
    // this will test the equality of the paths cross-platform
    assert(nuh.path.normalize == Paths.get("nuh.txt"))
    assert(zuh.path.normalize == Paths.get("zuh.txt.bar"))
    
    // can now interpolate the path variables on the command line
    assert(tool.commandLine === s"foo bar --is 3 4 5 baz --in $nuhPath --blarg $nuhPath $zuhPath")
  }

  testWithScriptContext("using() in any order with in() and out()") { implicit scriptContext =>
    val input1 = 42
    val input2 = "input2"
    val input3 = store("/inputStore").asInput
    val output = store("/outputStore")

    val useuse = "source /broad/software/scripts/useuse"
    val expectedCmdLineString =
      s"$useuse && reuse -q R-3.1 && " +
      s"(someTool --in 42 --in input2 --in ${fileSepForBash}inputStore --out ${fileSepForBash}outputStore)"

    val baseTool = cmd"someTool --in $input1 --in $input2 --in $input3 --out $output"

    val baseToolName = nameOf(baseTool)
    
    val toolv1 = baseTool.in(input3).out(output).using("R-3.1")
    
    val toolv1Name = nameOf(toolv1)

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

    val toolv2Name = nameOf(toolv2)
    
    assert(toolv2.commandLine === expectedCmdLineString)

    val toolv3 = baseTool.using("R-3.1").in(input3).out(output)

    val toolv3Name = nameOf(toolv3)
    
    assert(toolv3.commandLine === expectedCmdLineString)
    
    assert(baseToolName == toolv1Name)
    assert(baseToolName == toolv2Name)
    assert(baseToolName == toolv3Name)
  }

  testWithScriptContext("using() in a more complex cmd") { implicit scriptContext =>
    val input = store("/inputStore").asInput
    val output = store("/outputStore")
    val someOtherTool = "someOtherTool"

    val tool = cmd"(echo 10 ; sed '1d' $input | cut -f5- | sed 's/\t/ /g') > $output".using(someOtherTool)

    val useuse = "source /broad/software/scripts/useuse"
    val expected = s"$useuse && reuse -q someOtherTool && " +
      s"((echo 10 ; sed '1d' ${fileSepForBash}inputStore | cut -f5- | sed 's/\\t/ /g') > ${fileSepForBash}outputStore)"

    assert(tool.commandLine === expected)
    
    assert(nameOf(tool).isDefined)
  }

  testWithScriptContext("using() with multiple tools to be 'use'd") { implicit scriptContext =>
    val tool = cmd"someTool".using("otherTool1", "otherTool2", "otherTool3")

    val useuse = "source /broad/software/scripts/useuse"
    val expected = s"$useuse && reuse -q otherTool1 && reuse -q otherTool2 && reuse -q otherTool3 && (someTool)"

    assert(tool.commandLine === expected)
    
    assert(nameOf(tool).isDefined)
  }

  test("toToken") {
    TestHelpers.withScriptContext { implicit scriptContext =>
      import LoamCmdTool.toToken
      
      //Store
      val store0 = Store()
      val store1 = Store()
      
      assert(toToken(store0) === StoreToken(store0))
      
      //Non-HasLocation Iterable:
      assert(toToken(Nil) === MultiToken(Nil))
      assert(toToken(Seq(42)) === MultiToken(Seq(42)))
      assert(toToken(Seq("x", "y", "z")) === MultiToken(Seq("x", "y", "z")))
      
      //Iterable[Store]
      {
        val things = Seq(store0, store1)
        
        assert(toToken(things) === MultiStoreToken(things))
      }
      
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
  }
  
  test("in() and out() with no implicit i/o stores") {
    for (addInputsFirst <- Seq(true, false)) {
      TestHelpers.withScriptContext { implicit scriptContext =>
        val projectContext = scriptContext.projectContext
        
        val nStores = 4
        val stores = Seq.fill[Store](nStores)(Store())
  
        val tool = cmd"foo bar baz"
  
        val expectedTokens = tokens("foo bar baz")
        val inputsBefore: Set[Store] = Set.empty
        val outputsBefore: Set[Store] = Set.empty
        
        assertAddingIOStores(
            projectContext, 
            tool, 
            expectedTokens, 
            inputsBefore, 
            outputsBefore, 
            stores, 
            addInputsFirst)
      }
    }
  }

  test("in() and out() mixed with implicit i/o stores") {
    for (addInputsFirst <- Seq(true, false)) {
      TestHelpers.withScriptContext { implicit scriptContext =>
        val projectContext = scriptContext.projectContext

        val stores = Seq(store, store, store, store, store("inputFile.vcf").asInput, store("outputFile.txt"))
  
        val Seq(_, _, _, _, inStoreImplicit, outStoreImplicit) = stores 
  
        val tool = cmd"foo $inStoreImplicit $outStoreImplicit"
  
        val expectedTokens = tokens("foo ", inStoreImplicit, " ", outStoreImplicit)
        val inputsBefore = Set[Store](inStoreImplicit)
        val outputsBefore = Set[Store](outStoreImplicit)
        
        assertAddingIOStores(
            projectContext, 
            tool, 
            expectedTokens, 
            inputsBefore, 
            outputsBefore, 
            stores, 
            addInputsFirst)
      }
    }
  }

  testWithScriptContext("at(...) and asInput") { implicit scriptContext =>
    val inStoreWithPath = store("dir/inStoreWithPath.txt").asInput
    val outStoreWithPath = store("dir/outStoreWithPath.txt")
    val inStoreWithUri = store(uri("xyz://host/dir/inStoreWithUri")).asInput
    val outStoreWithUri = store(uri("xyz://host/dir/outStoreWithUri"))
    val tool = cmd"maker $inStoreWithPath $inStoreWithUri $outStoreWithPath $outStoreWithUri"
    val inPath = inStoreWithPath.render
    val outPath = outStoreWithPath.render
    val inUri = BashScript.escapeString(inStoreWithUri.uriOpt.get.toString)
    val outUri = BashScript.escapeString(outStoreWithUri.uriOpt.get.toString)
    
    val commandLineExpected = s"maker $inPath $inUri $outPath $outUri"
    
    assert(tool.commandLine === commandLineExpected)
  }
  
  testWithScriptContext("isStoreIterable") { implicit scriptContext =>
    import LoamCmdTool.isStoreIterable
    
    assert(isStoreIterable(Nil) === false)
    assert(isStoreIterable(Seq(42)) === false)
    assert(isStoreIterable(Seq("x", "y", "z")) === false)
    
    assert(isStoreIterable(Seq(store)) === true)
    assert(isStoreIterable(Seq(store, store)) === true)
    assert(isStoreIterable(Seq(store("foo.txt"), store("bar.vcf"))) === true)
  }
  
  private def storeMap(stores: Iterable[Store]): Map[LId, Store] = {
    stores.map(store => store.id -> store).toMap
  }

  private def tokens(values: AnyRef*): Seq[LoamToken] = values.map {
    case string: String => StringToken(string)
    case store: Store => StoreToken(store)
  }

  private def assertGraph(
      graph: LoamGraph, 
      tool: LoamCmdTool, 
      expectedTokens: Seq[LoamToken],
      expectedInputs: Map[LId, Store],
      expectedOutputs: Map[LId, Store]): Unit = {
    
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

  private def assertAddingIOStores(
      context: LoamProjectContext, 
      tool: LoamCmdTool, 
      expectedTokens: Seq[LoamToken],
      inputsBefore: Set[Store], 
      outputsBefore: Set[Store],
      stores: Seq[Store], 
      addInputsFirst: Boolean = true): Unit = {
    
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

    //DynamicConfig
    
    val config = DynamicConfig(ConfigFactory.parseString("foo { bar { baz = 42 } }"), Some("foo.bar.baz"))
    
    import LoamCmdTool.toToken
    
    assert(toToken(config) === StringToken("42"))
    
    val configThatShouldBlowUp = config.copy(pathOption = Some("blerg.zerg"))
    
    intercept[Exception] {
      toToken(configThatShouldBlowUp)
    }
    
    //arbitrary type
    final case class Foo(x: Int)
    
    assert(toToken(Foo(42)) === StringToken("Foo(42)"))
  }
}
