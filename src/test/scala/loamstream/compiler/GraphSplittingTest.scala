package loamstream.compiler

import org.scalatest.FunSuite
import loamstream.loam.LoamScript
import loamstream.TestHelpers
import loamstream.loam.LoamCmdTool
import loamstream.loam.LoamGraph
import loamstream.model.Tool
import loamstream.model.Store
import loamstream.util.Loggable

/**
 * @author clint
 * Jul 5, 2017
 */
final class GraphSplittingTest extends FunSuite with Loggable {
  import TestHelpers.config
  
  test("split simple loam file") {
    val code = """cmd"foo -i bar -o baz""""
    
    val chunkThunks = chunksFrom(code)
    
    assert(chunkThunks.size === 1)
    
    val graph = chunkThunks.head.apply()
    
    assert(graph.stores.isEmpty)
    
    assert(graph.tools.size === 1)
    
    val commandLine = graph.tools.head.asInstanceOf[LoamCmdTool].commandLine
    
    assert(commandLine === "foo -i bar -o baz")
  }
  
  test("split slightly-less-simple loam file") {
    val code = """
      val store0 = store[TXT].at("store0.txt").asInput
      val store1 = store[TXT].at("store1.txt")
      val store2 = store[TXT].at("store2.txt")
      
      cmd"foo -i $store0 -o $store1".in(store0).out(store1)
      
      val N = 3
      
      def countLines(s: Any): Int = N //NB: Don't depend on external files, for simplicity 
      
      andThen {
        val N = countLines(store1)
        
        for(i <- 1 to N) {
          val barOutput = store[TXT].at(s"bar-$i.txt")
        
          cmd"bar -i $store1 -o $barOutput".in(store1).out(barOutput)
        }
      }
      """
    
    val chunkThunks = chunksFrom(code)
    
    assert(chunkThunks.size === 2)
    
    val Seq(thunk0, thunk1) = chunkThunks.toSeq
    
    assert(thunk0() eq thunk0())
    assert(thunk1() eq thunk1())
    
    {
      val graph0 = thunk0()
      
      assert(graph0.stores.size === 3)
      
      assertStoreExists(graph0)("./store0.txt")
      assertStoreExists(graph0)("./store1.txt")
      assertStoreExists(graph0)("./store2.txt")
      
      assert(graph0.tools.size === 1)
      
      assertCommandExists(graph0)("foo -i ./store0.txt -o ./store1.txt")
      
      val store0 = findStore(graph0)("./store0.txt")
      val store1 = findStore(graph0)("./store1.txt")
      val store2 = findStore(graph0)("./store2.txt")
      
      val cmd0 = findCommand(graph0)("foo -i ./store0.txt -o ./store1.txt")
      
      assert(inputs(graph0)(cmd0) === Set[Store.Untyped](store0))
      
      assert(outputs(graph0)(cmd0) === Set[Store.Untyped](store1))
    }
    
    {
      val graph1 = thunk1()
      
      assert(graph1.stores.size === 6)
      
      assertStoreExists(graph1)("./store0.txt")
      assertStoreExists(graph1)("./store1.txt")
      assertStoreExists(graph1)("./store2.txt")
      assertStoreExists(graph1)("./bar-1.txt")
      assertStoreExists(graph1)("./bar-2.txt")
      assertStoreExists(graph1)("./bar-3.txt")
      
      assert(graph1.tools.size === 4)
      
      assertCommandExists(graph1)("foo -i ./store0.txt -o ./store1.txt")
      assertCommandExists(graph1)("bar -i ./store1.txt -o ./bar-1.txt")
      assertCommandExists(graph1)("bar -i ./store1.txt -o ./bar-2.txt")
      assertCommandExists(graph1)("bar -i ./store1.txt -o ./bar-3.txt")

      val store0 = findStore(graph1)("./store0.txt")
      val store1 = findStore(graph1)("./store1.txt")
      val store2 = findStore(graph1)("./store2.txt")
      val barOut1 = findStore(graph1)("./bar-1.txt")
      val barOut2 = findStore(graph1)("./bar-2.txt")
      val barOut3 = findStore(graph1)("./bar-3.txt")
      
      val cmd0 = findCommand(graph1)("foo -i ./store0.txt -o ./store1.txt")
      val bar1 = findCommand(graph1)("bar -i ./store1.txt -o ./bar-1.txt")
      val bar2 = findCommand(graph1)("bar -i ./store1.txt -o ./bar-2.txt")
      val bar3 = findCommand(graph1)("bar -i ./store1.txt -o ./bar-3.txt")
      
      assert(inputs(graph1)(cmd0) === Set[Store.Untyped](store0))
      assert(outputs(graph1)(cmd0) === Set[Store.Untyped](store1))
      
      assert(inputs(graph1)(bar1) === Set[Store.Untyped](store1))
      assert(outputs(graph1)(bar1) === Set[Store.Untyped](barOut1))
      
      assert(inputs(graph1)(bar2) === Set[Store.Untyped](store1))
      assert(outputs(graph1)(bar2) === Set[Store.Untyped](barOut2))
      
      assert(inputs(graph1)(bar3) === Set[Store.Untyped](store1))
      assert(outputs(graph1)(bar3) === Set[Store.Untyped](barOut3))
      
    }
  }
  
  private def chunksFrom(code: String): GraphSource = {
    val project = LoamProject(config, LoamScript("foo", code))
    
    val compiler = LoamCompiler(LoamCompiler.Settings.default)
    
    val result = compiler.compile(project)
    
    //TODO
    if(!result.isValid) {
      result.errors.map(_.toString).foreach(error(_))
    }
    
    require(result.isValid, "Compilation failed")
    
    val graphQueue = result.graphQueue
    
    assert(graphQueue.nonEmpty)
    
    graphQueue
  }

  private def toCommandLine(g: LoamGraph)(tool: Tool): String = tool.asInstanceOf[LoamCmdTool].commandLine
  
  private def assertCommandExists(g: LoamGraph)(expectedCl: String): Unit = {
    assert(g.tools.exists(t => toCommandLine(g)(t) == expectedCl))
  }
  
  private def findCommand(g: LoamGraph)(expectedCl: String): LoamCmdTool = {
    g.tools.find(t => toCommandLine(g)(t) == expectedCl).get.asInstanceOf[LoamCmdTool]
  }
    
  private def toPath(g: LoamGraph)(store: Store.Untyped): String = store.path.toString
  
  private def assertStoreExists(g: LoamGraph)(expectedPath: String): Unit = {
    assert(g.stores.exists(s => toPath(g)(s) == expectedPath))
  }
  
  private def findStore(g: LoamGraph)(expectedPath: String): Store[_] = {
    g.stores.find(s => toPath(g)(s) == expectedPath).get.asInstanceOf[Store[_]]
  }
  
  private def inputs(g: LoamGraph)(t: Tool): Set[Store.Untyped] = t.inputs.values.toSet[Store.Untyped]
  
  private def outputs(g: LoamGraph)(t: Tool): Set[Store.Untyped] = t.outputs.values.toSet[Store.Untyped]
}
