package loamstream.compiler

import org.scalatest.FunSuite
import loamstream.loam.LoamScriptContext
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.loam.LoamProjectContext
import loamstream.model.execute.ExecutionEnvironment

/**
 * @author clint
 * May 5, 2017
 */
final class LoamPredefTest extends FunSuite {
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
