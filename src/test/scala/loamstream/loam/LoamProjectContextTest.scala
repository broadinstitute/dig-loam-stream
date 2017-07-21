package loamstream.loam

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.util.ValueBox
import loamstream.compiler.GraphQueue

/**
 * @author clint
 * Jul 11, 2017
 */
final class LoamProjectContextTest extends FunSuite {

  import TestHelpers.config
  
  test("updateGraph") {
    val ctx = LoamProjectContext.empty(config)
    
    implicit val scriptCtx = new LoamScriptContext(LoamProjectContext.empty(config))
    
    val tool = makeTool("foo")
    
    assert(ctx.graph === LoamGraph.empty)
    
    ctx.updateGraph(_.withTool(tool, scriptCtx))
    
    assert(ctx.graph.tools === Set(tool))
  }
  
  test("registerGraphSoFar") {
    val ctx = LoamProjectContext.empty(config)
    
    implicit val scriptCtx = new LoamScriptContext(LoamProjectContext.empty(config))
    
    val toolFoo = makeTool("foo") 
    val toolBar = makeTool("bar")
    
    ctx.updateGraph(_.withTool(toolFoo, scriptCtx))
    
    val withToolFoo = ctx.graph
    
    assert(ctx.graph.tools === Set(toolFoo))
    
    val queue = ctx.graphQueue
    
    assert(queue.isEmpty === true)
    
    ctx.registerGraphSoFar()
    
    assert(queue.isEmpty === false)
    
    ctx.updateGraph(_.withTool(toolBar, scriptCtx))
    
    assert(ctx.graph.tools === Set(toolFoo, toolBar))
    
    ctx.registerGraphSoFar()
    
    assert(queue.isEmpty === false)
    
    assert(queue.dequeue().apply().tools === Set(toolFoo))
    
    assert(queue.dequeue().apply().tools === Set(toolFoo, toolBar))
  }
  
  test("registerLoamThunk") {
    val ctx = LoamProjectContext.empty(config)
    
    val queue = ctx.graphQueue
    
    implicit val scriptCtx = new LoamScriptContext(ctx)
    
    val toolFoo = makeTool("foo") 
    val toolBar = makeTool("bar")
    val toolBaz = makeTool("baz")
    
    /*
     * Something like:
     * 
     * cmd"foo"
     * 
     * andThen {
     *   cmd"bar"
     *   cmd"baz"  
     * }
     * 
     */
    
    ctx.updateGraph(_.withTool(toolFoo, scriptCtx))
    
    assert(ctx.graph.tools === Set(toolFoo))
    
    assert(queue.isEmpty === true)
    
    ctx.registerLoamThunk {
      ctx.updateGraph(_.withTool(toolBar, scriptCtx))
      ctx.updateGraph(_.withTool(toolBaz, scriptCtx))
    }
    
    assert(ctx.graph.tools === Set(toolFoo))
    assert(queue.isEmpty === false)
    
    val thunk = queue.dequeue()
    
    assert(queue.isEmpty === true)
    
    val futureGraph = thunk()
    
    assert(futureGraph.tools === Set(toolFoo, toolBar, toolBaz))
  }
  
  private def makeTool(commandLine: String)(implicit scriptCtx: LoamScriptContext): LoamCmdTool = {
    val t = LoamCmdTool.create()(identity)(scriptCtx, StringContext(commandLine))
    
    assert(t.commandLine === commandLine)
    
    t
  }
}
