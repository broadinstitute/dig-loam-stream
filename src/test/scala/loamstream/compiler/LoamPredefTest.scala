package loamstream.compiler

import org.scalatest.FunSuite
import loamstream.loam.LoamScriptContext
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.loam.LoamProjectContext
import loamstream.model.execute.Environment
import loamstream.loam.LoamCmdTool
import loamstream.TestHelpers
import loamstream.loam.LoamGraph
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.GoogleSettings
import loamstream.model.quantities.Memory
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.CpuTime
import loamstream.drm.uger.UgerDefaults
import loamstream.drm.DrmSystem
import loamstream.drm.lsf.LsfDefaults
import loamstream.drm.lsf.LsfDockerParams
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.UgerDrmSettings

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

  import Environment._
  
  test("google") {
    implicit val scriptContext = newScriptContext
    
    val settings = GoogleSettings(scriptContext.googleConfig.clusterId)
    
    doEeTest(scriptContext, Local, Google(settings), LoamPredef.google)
  }
  
  test("local") {
    implicit val scriptContext = newScriptContext
    
    doEeTest(scriptContext, Uger(TestHelpers.defaultUgerSettings), Local, LoamPredef.local)
  }
  
  test("uger") {
    implicit val scriptContext = newScriptContext(DrmSystem.Uger)
    
    doEeTest(scriptContext, Local, Uger(TestHelpers.defaultUgerSettings), LoamPredef.uger)
  }
  
  test("drm") {
    {
      implicit val scriptContext = newScriptContext(DrmSystem.Uger)
    
      doEeTest(scriptContext, Local, Uger(TestHelpers.defaultUgerSettings), LoamPredef.drm)
    }
    
    {
      implicit val scriptContext = newScriptContext(DrmSystem.Lsf)
    
      doEeTest(scriptContext, Local, Lsf(TestHelpers.defaultLsfSettings), LoamPredef.drm)
    }
  }
  
  test("ugerWith - defaults") {
    implicit val scriptContext = newScriptContext(DrmSystem.Uger)
    
    val ugerConfig = scriptContext.ugerConfig
    
    assert(ugerConfig.defaultCores !== UgerDefaults.cores)
    assert(ugerConfig.defaultMemoryPerCore !== UgerDefaults.memoryPerCore)
    assert(ugerConfig.defaultMaxRunTime !== UgerDefaults.maxRunTime)
    
    //Make sure defaults come from LoamConfig
    val expectedSettings = DrmSystem.Uger.settingsMaker(
        ugerConfig.defaultCores,
        ugerConfig.defaultMemoryPerCore,
        ugerConfig.defaultMaxRunTime,
        Option(UgerDefaults.queue),
        None)
    
    doEeTest(scriptContext, Local, Uger(expectedSettings), LoamPredef.ugerWith())
  }
  
  test("drmWith - defaults - Uger") {
    implicit val scriptContext = newScriptContext(DrmSystem.Uger)
    
    val ugerConfig = scriptContext.ugerConfig
    
    assert(ugerConfig.defaultCores !== UgerDefaults.cores)
    assert(ugerConfig.defaultMemoryPerCore !== UgerDefaults.memoryPerCore)
    assert(ugerConfig.defaultMaxRunTime !== UgerDefaults.maxRunTime)
    
    //Make sure defaults come from LoamConfig
    val expectedSettings = DrmSystem.Uger.settingsMaker(
        ugerConfig.defaultCores,
        ugerConfig.defaultMemoryPerCore,
        ugerConfig.defaultMaxRunTime,
        Option(UgerDefaults.queue),
        None)
    
    doEeTest(scriptContext, Local, Uger(expectedSettings), LoamPredef.drmWith())
  }
  
  test("drmWith - defaults - LSF") {
    implicit val scriptContext = newScriptContext(DrmSystem.Lsf)
    
    val lsfConfig = scriptContext.lsfConfig
    
    assert(lsfConfig.defaultCores !== LsfDefaults.cores)
    assert(lsfConfig.defaultMemoryPerCore !== LsfDefaults.memoryPerCore)
    assert(lsfConfig.defaultMaxRunTime !== LsfDefaults.maxRunTime)
    
    //Make sure defaults come from LoamConfig
    val expectedSettings = DrmSystem.Lsf.settingsMaker(
        lsfConfig.defaultCores,
        lsfConfig.defaultMemoryPerCore,
        lsfConfig.defaultMaxRunTime,
        None,
        None)
    
    doEeTest(scriptContext, Local, Lsf(expectedSettings), LoamPredef.drmWith())
  }
  
  test("ugerWith - non-defaults") {
    implicit val scriptContext = newScriptContext(DrmSystem.Uger)
    
    import TestHelpers.path
    
    val expectedSettings = UgerDrmSettings(
        cores = Cpus(2), 
        memoryPerCore = Memory.inGb(4), 
        maxRunTime = CpuTime.inHours(6), 
        queue = Option(UgerDefaults.queue),
        None)
    
    doEeTest(
        scriptContext, 
        Local, 
        Uger(expectedSettings), 
        LoamPredef.ugerWith(2, 4, 6))
  }
  
  test("drmWith - non-defaults - Uger") {
    implicit val scriptContext = newScriptContext(DrmSystem.Uger)
    
    import TestHelpers.path
    
    val expectedSettings = UgerDrmSettings(
        cores = Cpus(2), 
        memoryPerCore = Memory.inGb(4), 
        maxRunTime = CpuTime.inHours(6), 
        queue = Option(UgerDefaults.queue),
        None)
    
    doEeTest(
        scriptContext, 
        Local, 
        Uger(expectedSettings), 
        LoamPredef.drmWith(2, 4, 6))
  }
  
  test("drmWith - non-defaults - Lsf") {
    implicit val scriptContext = newScriptContext(DrmSystem.Lsf)
    
    import TestHelpers.path
    
    val expectedSettings = LsfDrmSettings(
        cores = Cpus(2), 
        memoryPerCore = Memory.inGb(4), 
        maxRunTime = CpuTime.inHours(6), 
        queue = None,
        Some(LsfDockerParams(imageName = "library/foo:1.2.3")))
    
    doEeTest(
        scriptContext, 
        Local, 
        Lsf(expectedSettings), 
        LoamPredef.drmWith(2, 4, 6, "library/foo:1.2.3"))
  }
  
  test("nonexistentPaht doesn't exist") {
    import java.nio.file.Files.exists
    
    //Lame, but marginally better than nothing
    assert(exists(LoamPredef.nonExistentPath) === false)
  }
  
  private def newScriptContext: LoamScriptContext = new LoamScriptContext(TestHelpers.emptyProjectContext)
  
  private def newScriptContext(drmSystem: DrmSystem): LoamScriptContext = {
    new LoamScriptContext(TestHelpers.emptyProjectContext(drmSystem))
  }
  
  private def doEeTest[A](
      scriptContext: LoamScriptContext,
      initial: Environment, 
      shouldHaveSwitchedTo: Environment,
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
