package loamstream.compiler

import org.scalatest.FunSuite
import loamstream.loam.LoamScriptContext
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.loam.LoamProjectContext
import loamstream.model.execute.ExecutionEnvironment
import loamstream.loam.LoamCmdTool
import loamstream.TestHelpers
import loamstream.loam.LoamGraph

/**
 * @author clint
 * May 5, 2017
 */
final class LoamPredefTest extends FunSuite {
  test("andThen") {
    import TestHelpers.config
    
    def makeTool(commandLine: String)(implicit scriptCtx: LoamScriptContext): LoamCmdTool = {
      val t = LoamCmdTool.create()(identity)(scriptCtx, StringContext(commandLine))
    
      assert(t.commandLine === commandLine)
    
      t
    }
    
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
    val g0 = ctx.graph 
    
    assert(g0 === LoamGraph.empty)
    
    ctx.updateGraph(_.withTool(toolFoo, scriptCtx))
    
    val g1 = ctx.graph
    
    assert(g1.tools === Set(toolFoo))
    
    assert(queue.isEmpty === true)
    
    LoamPredef.andThen {
      ctx.updateGraph(_.withTool(toolBar, scriptCtx))
      ctx.updateGraph(_.withTool(toolBaz, scriptCtx))
    }
    
    assert(queue.isEmpty === false)
    
    assert(queue.dequeue().apply() === g1)
    
    assert(queue.isEmpty === false)
    
    assert(queue.dequeue().apply().tools === Set(toolFoo, toolBar, toolBaz))
    
    assert(queue.isEmpty === true)
  }
  
  test("configFromFile") {
    val config = LoamPredef.loadConfig("src/test/resources/foo.config")
    
    //Config file should have been loaded BUT NOT merged with defaults
    
    import net.ceedubs.ficus.Ficus._
    
    //default from reference.conf, shouldn't have been loaded
    intercept[Exception] {
      config.loamstream.uger.logFile.as[String]
    }
    
    //new key
    assert(config.loamstream.uger.maxNumJobs.as[Int] === 42)
    
    //new key
    assert(config.loamstream.uger.foo.as[String] === "bar")
    
    //new key
    assert(config.loamstream.nuh.as[String] === "zuh")
  }
  
  import ExecutionEnvironment._
  
  test("google") {
    implicit val scriptContext = newScriptContext
    
    doEeTest(scriptContext, Local, Google, LoamPredef.google)
  }
  
  test("local") {
    implicit val scriptContext = newScriptContext
    
    doEeTest(scriptContext, Uger, Local, LoamPredef.local)
  }
  
  test("uger") {
    implicit val scriptContext = newScriptContext
    
    doEeTest(scriptContext, Local, Uger, LoamPredef.uger)
  }
  
  private def newScriptContext: LoamScriptContext = {
    val projectContext = LoamProjectContext.empty(LoamConfig(None, None, None, None, None, ExecutionConfig.default))
    
    new LoamScriptContext(projectContext)
  }
  
  private def doEeTest[A](
      scriptContext: LoamScriptContext,
      initial: ExecutionEnvironment, 
      shouldHaveSwitchedTo: ExecutionEnvironment,
      switchEe: (=> Any) => Any): Unit = {
    
    scriptContext.executionEnvironment = initial
    
    assert(scriptContext.executionEnvironment === initial)
    
    switchEe {
      //We should have switched to the new EE
      assert(scriptContext.executionEnvironment === shouldHaveSwitchedTo)
    }
    
    //We should have restored the original EE 
    assert(scriptContext.executionEnvironment === initial)
  }
}
