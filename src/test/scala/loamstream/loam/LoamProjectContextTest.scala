package loamstream.loam

import org.scalatest.FunSuite

import loamstream.TestHelpers

/**
 * @author clint
 * Jul 11, 2017
 */
final class LoamProjectContextTest extends FunSuite {

  import loamstream.TestHelpers.config
  
  test("updateGraph") {
    val ctx = LoamProjectContext.empty(config)
    
    implicit val scriptCtx = new LoamScriptContext(LoamProjectContext.empty(config))
    
    val tool = makeTool("foo")
    
    assert(ctx.graph === LoamGraph.empty)
    
    ctx.updateGraph(_.withTool(tool, scriptCtx))
    
    assert(ctx.graph.tools === Set(tool))
  }
  
  private def makeTool(commandLine: String)(implicit scriptCtx: LoamScriptContext): LoamCmdTool = {
    val t = LoamCmdTool.create()(identity)(scriptCtx, StringContext(commandLine))
    
    assert(t.commandLine === commandLine)
    
    t
  }
}
