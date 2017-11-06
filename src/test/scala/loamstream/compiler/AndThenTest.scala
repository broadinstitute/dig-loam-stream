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
final class AndThenTest extends FunSuite with Loggable {
  import TestHelpers.config
  
  test("split simple loam file") {
    val code = """cmd"foo -i bar -o baz""""
    
    val chunkThunks = chunksFrom(code).toVector
    
    assert(chunkThunks.size === 1)
    
    val graph = chunkThunks.head.apply()
    
    assert(graph.stores.isEmpty)
    
    assert(graph.tools.size === 1)
    
    val commandLine = graph.tools.head.asInstanceOf[LoamCmdTool].commandLine
    
    assert(commandLine === "foo -i bar -o baz")
  }
  
  test("split slightly-less-simple loam file") {
    val code = """
      val store0 = store.at("store0.txt").asInput
      val store1 = store.at("store1.txt")
      val store2 = store.at("store2.txt")
      
      cmd"foo -i $store0 -o $store1".in(store0).out(store1)
      
      val N = 3
      
      def countLines(s: Any): Int = N //NB: Don't depend on external files, for simplicity 
      
      andThen {
        val N = countLines(store1)
        
        for(i <- 1 to N) {
          val barOutput = store.at(s"bar-$i.txt")
        
          cmd"bar -i $store1 -o $barOutput".in(store1).out(barOutput)
        }
      }
      """
    
    val chunkThunks = chunksFrom(code).toVector
    
    assert(chunkThunks.size === 3)
    
    val Seq(thunk0, thunk1, thunk2) = chunkThunks
    
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
      
      assert(inputs(graph0)(cmd0) === Set[Store](store0))
      
      assert(outputs(graph0)(cmd0) === Set[Store](store1))
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
      
      assert(inputs(graph1)(cmd0) === Set[Store](store0))
      assert(outputs(graph1)(cmd0) === Set[Store](store1))
      
      assert(inputs(graph1)(bar1) === Set[Store](store1))
      assert(outputs(graph1)(bar1) === Set[Store](barOut1))
      
      assert(inputs(graph1)(bar2) === Set[Store](store1))
      assert(outputs(graph1)(bar2) === Set[Store](barOut2))
      
      assert(inputs(graph1)(bar3) === Set[Store](store1))
      assert(outputs(graph1)(bar3) === Set[Store](barOut3))
      
    }
    
    //Final graph should have no new tools
    
    val graph1 = thunk1()
    val graph2 = thunk2()
    
    val filteredGraph2 = graph2.without(graph1.tools)
    
    assert(filteredGraph2.tools === Set.empty)
  }
  
  test("split loam file with commands before and after andThen") {
    val code = """
      import scala.collection.mutable.{Buffer,ArrayBuffer}
      
      val store0 = store.at("store0.txt").asInput
      val store1 = store.at("store1.txt")
      val store2 = store.at("store2.txt")
      val barOutputs: Buffer[Store] = new ArrayBuffer
      val store3 = store.at("store3.txt")
      
      cmd"foo -i $store0 -o $store1".in(store0).out(store1)
      
      val N = 3
      
      def countLines(s: Any): Int = N //NB: Don't depend on external files, for simplicity 
      
      andThen {
        val N = countLines(store1)
        
        for(i <- 1 to N) {
          val barOutput = store.at(s"bar-$i.txt")
        
          barOutputs += barOutput
        
          cmd"bar -i $store1 -o $barOutput".in(store1).out(barOutput)
        }
      }
      
      cmd"baz -i $store2 -o $store3".in(store2).out(store3)
      """
    
    val chunkThunks = chunksFrom(code).toVector
    
    assert(chunkThunks.size === 3)
    
    val Seq(thunk0, thunk1, thunk2) = chunkThunks
    
    assert(thunk0() eq thunk0())
    assert(thunk1() eq thunk1())
    assert(thunk2() eq thunk2())
    
    assert(thunk0() != thunk1())
    assert(thunk1() != thunk2())
    assert(thunk2() != thunk0())
    
    //graph0 should contain everything defined BEFORE the andThen
    {
      val graph0 = thunk0()
      
      assert(graph0.stores.size === 4)
      
      val store0 = findStore(graph0)("./store0.txt")
      val store1 = findStore(graph0)("./store1.txt")
      val store2 = findStore(graph0)("./store2.txt")
      val store3 = findStore(graph0)("./store3.txt")
      
      assert(graph0.tools.size === 1)
      
      val cmd0 = findCommand(graph0)("foo -i ./store0.txt -o ./store1.txt")
      
      assert(inputs(graph0)(cmd0) === Set[Store](store0))
      assert(outputs(graph0)(cmd0) === Set[Store](store1))
    }
    
    //graph1 should contain everything defined by running the code OUTSIDE the andThen,
    //AND the code INSIDE the andThen
    {
      val graph1 = thunk1()
      
      assert(graph1.stores.size === 7)
      
      val store0 = findStore(graph1)("./store0.txt")
      val store1 = findStore(graph1)("./store1.txt")
      val store2 = findStore(graph1)("./store2.txt")
      val store3 = findStore(graph1)("./store3.txt")
      val barOut1 = findStore(graph1)("./bar-1.txt")
      val barOut2 = findStore(graph1)("./bar-2.txt")
      val barOut3 = findStore(graph1)("./bar-3.txt")
      
      assert(graph1.tools.size === 5)
      
      val cmd0 = findCommand(graph1)("foo -i ./store0.txt -o ./store1.txt")
      val bar1 = findCommand(graph1)("bar -i ./store1.txt -o ./bar-1.txt")
      val bar2 = findCommand(graph1)("bar -i ./store1.txt -o ./bar-2.txt")
      val bar3 = findCommand(graph1)("bar -i ./store1.txt -o ./bar-3.txt")
      val cmd1 = findCommand(graph1)("baz -i ./store2.txt -o ./store3.txt")
      
      assert(inputs(graph1)(cmd0) === Set[Store](store0))
      assert(outputs(graph1)(cmd0) === Set[Store](store1))
      
      assert(inputs(graph1)(bar1) === Set[Store](store1))
      assert(outputs(graph1)(bar1) === Set[Store](barOut1))
      
      assert(inputs(graph1)(bar2) === Set[Store](store1))
      assert(outputs(graph1)(bar2) === Set[Store](barOut2))
      
      assert(inputs(graph1)(bar3) === Set[Store](store1))
      assert(outputs(graph1)(bar3) === Set[Store](barOut3))
      
      assert(inputs(graph1)(cmd1) === Set[Store](store2))
      assert(outputs(graph1)(cmd1) === Set[Store](store3))
    }
    
    //graph2 should contain everything defined OUTSIDE the andThen; that is, everything defined by
    //running the loam code to the end, BUT NOT YET running the andThen block.
    {
      val graph2 = thunk2()
      
      val addedSinceGraph1 = graph2.without(thunk1().tools)
      
      assert(addedSinceGraph1.tools.isEmpty)
      
      assert(graph2.stores.size === 4)
      
      val store0 = findStore(graph2)("./store0.txt")
      val store1 = findStore(graph2)("./store1.txt")
      val store2 = findStore(graph2)("./store2.txt")
      val store3 = findStore(graph2)("./store3.txt")

      assert(graph2.tools.size === 2)
      
      val cmd0 = findCommand(graph2)("foo -i ./store0.txt -o ./store1.txt")
      val cmd1 = findCommand(graph2)("baz -i ./store2.txt -o ./store3.txt")

      val addedSinceGraph0 = graph2.without(thunk0().tools)
      
      assert(addedSinceGraph0.tools === Set(cmd1))
      
      assert(inputs(graph2)(cmd0) === Set[Store](store0))
      assert(outputs(graph2)(cmd0) === Set[Store](store1))
      
      assert(inputs(graph2)(cmd1) === Set[Store](store2))
      assert(outputs(graph2)(cmd1) === Set[Store](store3))
    }
  }
  
  test("split loam file with multiple andThens") {
    val code = """
      val store0 = store.at("store0.txt").asInput
      val store1 = store.at("store1.txt")
      val store2 = store.at("store2.txt")
      val store3 = store.at("store3.txt")
      
      cmd"foo -i $store0 -o $store1".in(store0).out(store1)
      
      def countLines(s: Any): Int = 3 //NB: Don't depend on external files, for simplicity 
      
      andThen {
        val N = countLines(store1)
        
        for(i <- 1 to N) {
          val barOutput = store.at(s"bar-$i.txt")
        
          cmd"bar -i $store1 -o $barOutput".in(store1).out(barOutput)
        }
      }
      
      cmd"baz -i $store2 -o $store3".in(store2).out(store3)
      
      andThen {
        val N = countLines(store3)
        
        for(i <- 1 to N) {
          val nuhOutput = store.at(s"nuh-$i.txt")
        
          cmd"nuh -i $store3 -o $nuhOutput".in(store3).out(nuhOutput)
        }
      }
      """
    
    val chunkThunks = chunksFrom(code).toVector
    
    assert(chunkThunks.size === 5)
    
    val Seq(thunk0, thunk1, thunk2, thunk3, thunk4) = chunkThunks
    
    //graph0 should contain everything defined BEFORE the FIRST andThen
    {
      val graph0 = thunk0()
      
      assert(graph0.stores.size === 4)
      
      val store0 = findStore(graph0)("./store0.txt")
      val store1 = findStore(graph0)("./store1.txt")
      val store2 = findStore(graph0)("./store2.txt")
      val store3 = findStore(graph0)("./store3.txt")
      
      assert(graph0.tools.size === 1)
      
      val cmd0 = findCommand(graph0)("foo -i ./store0.txt -o ./store1.txt")
      
      assert(inputs(graph0)(cmd0) === Set[Store](store0))
      assert(outputs(graph0)(cmd0) === Set[Store](store1))
    }
    
    //graph1 should contain everything defined by running the code OUTSIDE the andThens,
    //AND the code INSIDE the FIRST andThen block
    {
      val graph1 = thunk1()
      
      assert(graph1.stores.size === 7)
      
      val store0 = findStore(graph1)("./store0.txt")
      val store1 = findStore(graph1)("./store1.txt")
      val store2 = findStore(graph1)("./store2.txt")
      val store3 = findStore(graph1)("./store3.txt")
      val barOut1 = findStore(graph1)("./bar-1.txt")
      val barOut2 = findStore(graph1)("./bar-2.txt")
      val barOut3 = findStore(graph1)("./bar-3.txt")
      
      assert(graph1.tools.size === 5)
      
      val cmd0 = findCommand(graph1)("foo -i ./store0.txt -o ./store1.txt")
      val bar1 = findCommand(graph1)("bar -i ./store1.txt -o ./bar-1.txt")
      val bar2 = findCommand(graph1)("bar -i ./store1.txt -o ./bar-2.txt")
      val bar3 = findCommand(graph1)("bar -i ./store1.txt -o ./bar-3.txt")
      val cmd1 = findCommand(graph1)("baz -i ./store2.txt -o ./store3.txt")
      
      assert(inputs(graph1)(cmd0) === Set[Store](store0))
      assert(outputs(graph1)(cmd0) === Set[Store](store1))
      
      assert(inputs(graph1)(bar1) === Set[Store](store1))
      assert(outputs(graph1)(bar1) === Set[Store](barOut1))
      
      assert(inputs(graph1)(bar2) === Set[Store](store1))
      assert(outputs(graph1)(bar2) === Set[Store](barOut2))
      
      assert(inputs(graph1)(bar3) === Set[Store](store1))
      assert(outputs(graph1)(bar3) === Set[Store](barOut3))
      
      assert(inputs(graph1)(cmd1) === Set[Store](store2))
      assert(outputs(graph1)(cmd1) === Set[Store](store3))
    }
    
    //graph2 should contain everything defined BEFORE the SECOND andThen
    {
      val graph2 = thunk2()
      
      val addedSinceGraph1 = graph2.without(thunk1().tools)
      
      assert(addedSinceGraph1.tools.isEmpty)
      
      assert(graph2.stores.size === 4)
      
      val store0 = findStore(graph2)("./store0.txt")
      val store1 = findStore(graph2)("./store1.txt")
      val store2 = findStore(graph2)("./store2.txt")
      val store3 = findStore(graph2)("./store3.txt")

      assert(graph2.tools.size === 2)
      
      val cmd0 = findCommand(graph2)("foo -i ./store0.txt -o ./store1.txt")
      val cmd1 = findCommand(graph2)("baz -i ./store2.txt -o ./store3.txt")

      val addedSinceGraph0 = graph2.without(thunk0().tools)
      
      assert(addedSinceGraph0.tools === Set(cmd1))
      
      assert(inputs(graph2)(cmd0) === Set[Store](store0))
      assert(outputs(graph2)(cmd0) === Set[Store](store1))
      
      assert(inputs(graph2)(cmd1) === Set[Store](store2))
      assert(outputs(graph2)(cmd1) === Set[Store](store3))
    }
    
    //graph3 should contain everything defined by running the code OUTSIDE the andThens,
    //AND the code INSIDE the FIRST AND SECOND andThens
    {
      val graph3 = thunk3()
      
      assert(graph3.stores.size === 10)
      
      val store0 = findStore(graph3)("./store0.txt")
      val store1 = findStore(graph3)("./store1.txt")
      val store2 = findStore(graph3)("./store2.txt")
      val store3 = findStore(graph3)("./store3.txt")
      val barOut1 = findStore(graph3)("./bar-1.txt")
      val barOut2 = findStore(graph3)("./bar-2.txt")
      val barOut3 = findStore(graph3)("./bar-3.txt")
      val nuhOut1 = findStore(graph3)("./nuh-1.txt")
      val nuhOut2 = findStore(graph3)("./nuh-2.txt")
      val nuhOut3 = findStore(graph3)("./nuh-3.txt")
      
      assert(graph3.tools.size === 8)
      
      val cmd0 = findCommand(graph3)("foo -i ./store0.txt -o ./store1.txt")
      val bar1 = findCommand(graph3)("bar -i ./store1.txt -o ./bar-1.txt")
      val bar2 = findCommand(graph3)("bar -i ./store1.txt -o ./bar-2.txt")
      val bar3 = findCommand(graph3)("bar -i ./store1.txt -o ./bar-3.txt")
      val cmd1 = findCommand(graph3)("baz -i ./store2.txt -o ./store3.txt")
      val nuh1 = findCommand(graph3)("nuh -i ./store3.txt -o ./nuh-1.txt")
      val nuh2 = findCommand(graph3)("nuh -i ./store3.txt -o ./nuh-2.txt")
      val nuh3 = findCommand(graph3)("nuh -i ./store3.txt -o ./nuh-3.txt")
      
      assert(inputs(graph3)(cmd0) === Set[Store](store0))
      assert(outputs(graph3)(cmd0) === Set[Store](store1))
      
      assert(inputs(graph3)(bar1) === Set[Store](store1))
      assert(outputs(graph3)(bar1) === Set[Store](barOut1))
      
      assert(inputs(graph3)(bar2) === Set[Store](store1))
      assert(outputs(graph3)(bar2) === Set[Store](barOut2))
      
      assert(inputs(graph3)(bar3) === Set[Store](store1))
      assert(outputs(graph3)(bar3) === Set[Store](barOut3))
      
      assert(inputs(graph3)(cmd1) === Set[Store](store2))
      assert(outputs(graph3)(cmd1) === Set[Store](store3))
      
      assert(inputs(graph3)(nuh1) === Set[Store](store3))
      assert(outputs(graph3)(nuh1) === Set[Store](nuhOut1))
      
      assert(inputs(graph3)(nuh2) === Set[Store](store3))
      assert(outputs(graph3)(nuh2) === Set[Store](nuhOut2))
      
      assert(inputs(graph3)(nuh3) === Set[Store](store3))
      assert(outputs(graph3)(nuh3) === Set[Store](nuhOut3))
    }
    
    //graph4 should contain everything defined OUTSIDE BOTH andThens; that is, everything defined by
    //running the loam code to the end, BUT NOT YET running the code in either andThen block.
    {
      val graph4 = thunk4()
      
      val addedSinceGraph3 = graph4.without(thunk3().tools)
      
      assert(addedSinceGraph3.tools.isEmpty)
      
      val addedSinceGraph2 = graph4.without(thunk2().tools)
      
      assert(addedSinceGraph2.tools.isEmpty)
      
      assert(graph4.stores.size === 4)
      
      val store0 = findStore(graph4)("./store0.txt")
      val store1 = findStore(graph4)("./store1.txt")
      val store2 = findStore(graph4)("./store2.txt")
      val store3 = findStore(graph4)("./store3.txt")

      assert(graph4.tools.size === 2)
      
      val cmd0 = findCommand(graph4)("foo -i ./store0.txt -o ./store1.txt")
      val cmd1 = findCommand(graph4)("baz -i ./store2.txt -o ./store3.txt")

      val addedSinceGraph0 = graph4.without(thunk0().tools)
      
      assert(addedSinceGraph0.tools === Set(cmd1))
      
      assert(inputs(graph4)(cmd0) === Set[Store](store0))
      assert(outputs(graph4)(cmd0) === Set[Store](store1))
      
      assert(inputs(graph4)(cmd1) === Set[Store](store2))
      assert(outputs(graph4)(cmd1) === Set[Store](store3))
    }
  }
  
  test("one andThen, at the end, but it throws") {
    val code = """
      val store0 = store.at("store0.txt").asInput
      val store1 = store.at("store1.txt")
      val store2 = store.at("store2.txt")
      
      cmd"foo -i $store0 -o $store1".in(store0).out(store1)
      
      andThen {
        throw new Exception("blerg")
      }
      """
    
    val chunkThunks = chunksFrom(code).toVector
    
    //We should still get 3 chunks
    assert(chunkThunks.size === 3)
    
    val Seq(thunk0, thunk1, thunk2) = chunkThunks
    
    assert(thunk0() eq thunk0())
    
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
      
      assert(inputs(graph0)(cmd0) === Set(store0))
      
      assert(outputs(graph0)(cmd0) === Set(store1))
    }
    
    {
      val caught = intercept[Exception] {
        thunk1()
      }
      
      assert(caught.getMessage === "blerg")
    }
    
    //Final graph should have no new tools
    
    val graph2 = thunk2()
    
    val filteredGraph2 = graph2.without(thunk0().tools)
    
    assert(filteredGraph2.tools === Set.empty)
  }
  
  private def chunksFrom(code: String): Iterator[GraphThunk] = {
    val project = LoamProject(config, LoamScript("foo", code))
    
    val compiler = LoamCompiler(LoamCompiler.Settings.default)
    
    val result = compiler.compile(project)
    
    //TODO
    if(!result.isValid) {
      result.errors.map(_.toString).foreach(error(_))
    }
    
    assert(result.isValid === true)
    
    val graphs = result.graphSource.iterator
    
    assert(graphs.hasNext)
    
    graphs
  }

  private def toCommandLine(g: LoamGraph)(tool: Tool): String = tool.asInstanceOf[LoamCmdTool].commandLine
  
  private def assertCommandExists(g: LoamGraph)(expectedCl: String): Unit = {
    assert(g.tools.exists(t => toCommandLine(g)(t) == expectedCl))
  }
  
  private def findCommand(g: LoamGraph)(expectedCl: String): LoamCmdTool = {
    g.tools.find(t => toCommandLine(g)(t) == expectedCl).get.asInstanceOf[LoamCmdTool]
  }
    
  private def toPath(g: LoamGraph)(store: Store): String = store.path.toString
  
  private def assertStoreExists(g: LoamGraph)(expectedPath: String): Unit = {
    assert(g.stores.exists(s => toPath(g)(s) == expectedPath))
  }
  
  private def findStore(g: LoamGraph)(expectedPath: String): Store = {
    g.stores.find(s => toPath(g)(s) == expectedPath).get.asInstanceOf[Store]
  }
  
  private def inputs(g: LoamGraph)(t: Tool): Set[Store] = t.inputs.values.toSet[Store]
  
  private def outputs(g: LoamGraph)(t: Tool): Set[Store] = t.outputs.values.toSet[Store]
}
