package loamstream.loam

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.LsSettings

/**
 * @author clint
 * Jul 11, 2017
 */
final class LoamProjectContextTest extends FunSuite {

  import loamstream.TestHelpers.config
  
  test("updateGraph") {
    val ctx = LoamProjectContext.empty(config, LsSettings.noCliConfig)
    
    implicit val scriptCtx = new LoamScriptContext(ctx)
    
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
