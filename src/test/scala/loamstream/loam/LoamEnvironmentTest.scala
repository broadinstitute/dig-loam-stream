package loamstream.loam

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.Environment
import loamstream.model.execute.GoogleSettings
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.drm.uger.UgerDefaults
import loamstream.util.Loggable
import loamstream.drm.DrmSystem
import loamstream.model.execute.EnvironmentType

/**
 * @author clint
 * Nov 22, 2016
 */
final class LoamEnvironmentTest extends FunSuite with Loggable {
  
  private val clusterId = TestHelpers.config.googleConfig.get.clusterId
  
  private def commandLineFrom(j: LJob): String = j.asInstanceOf[CommandLineJob].commandLineString
  
  test("Default EE") {
    val graph = TestHelpers.makeGraph { implicit context => 
      import LoamCmdTool._
      
      cmd"foo"
    }
    
    val executable = LoamEngine.toExecutable(graph)
    
    val job = executable.jobs.head
    
    assert(executable.jobs.size === 1)
    assert(commandLineFrom(job) === "foo")
    assert(job.executionEnvironment === Environment.Local)
  }
  
  private def makeGraph(env: Environment): (LoamScriptContext => Any) => LoamGraph = env.tpe match {
    case EnvironmentType.Uger => TestHelpers.makeGraph(DrmSystem.Uger)(_)
    case EnvironmentType.Lsf => TestHelpers.makeGraph(DrmSystem.Lsf)(_)
    case _ => TestHelpers.makeGraph(_)
  }
  
  private def makeGraph(drmSystem: DrmSystem): (LoamScriptContext => Any) => LoamGraph = {
    TestHelpers.makeGraph(drmSystem)(_)
  }
  
  test("Set EE") {
    def doTest(env: Environment): Unit = {
      val methodName = env.toString.toLowerCase
      
      val graph = makeGraph(env) { implicit context => 
        import LoamCmdTool._
      
        envFn(env) {
          cmd"foo"
        }
      }
      
      val executable = LoamEngine.toExecutable(graph)
      
      val job = executable.jobs.head
      
      assert(executable.jobs.size === 1)
      assert(commandLineFrom(job) === "foo")  
      assert(job.executionEnvironment === env)
    }
    
    doTest(Environment.Local)
    doTest(Environment.Uger(DrmSettings(Cpus(2), Memory.inGb(3), CpuTime.inHours(4), Option(UgerDefaults.queue))))
    doTest(Environment.Lsf(DrmSettings(Cpus(3), Memory.inGb(4), CpuTime.inHours(5), None)))
    doTest(Environment.Google(GoogleSettings(clusterId)))
  }
  
  test("Multiple EEs") {
    def doTest(drmSystem: DrmSystem, env1: Environment, env2: Environment, env3: Environment ): Unit = {
      val methodName1 = env1.toString.toLowerCase
      val methodName2 = env2.toString.toLowerCase
      val methodName3 = env3.toString.toLowerCase
      
      val graph = makeGraph(drmSystem) { implicit context => 
        import LoamCmdTool._
        
        envFn(env1) {
          cmd"foo"
          cmd"bar"
        }
        
        envFn(env2) {
          cmd"baz"
        }
        
        envFn(env3) {
          cmd"blerg"
          cmd"zerg"
        }
      }
      
      val executable = LoamEngine.toExecutable(graph)
      
      val jobs = executable.jobs
      
      def jobWith(commandLine: String): LJob = jobs.find(j => commandLineFrom(j.job) == commandLine).get.job
      
      val foo = jobWith("foo")
      val bar = jobWith("bar")
      val baz = jobWith("baz")
      val blerg = jobWith("blerg")
      val zerg = jobWith("zerg")
      
      assert(jobs.size === 5)
        
      assert(foo.executionEnvironment === env1)
      assert(bar.executionEnvironment === env1)
      
      assert(baz.executionEnvironment === env2)
      
      assert(blerg.executionEnvironment === env3)
      assert(zerg.executionEnvironment === env3)
    }
    
    import Environment._
    
    val ugerEnv = Uger(TestHelpers.defaultUgerSettings)
    val lsfEnv = Lsf(TestHelpers.defaultLsfSettings)
    val googleEnv = Google(GoogleSettings(clusterId))
    
    doTest(DrmSystem.Uger, Local, ugerEnv, googleEnv)
    doTest(DrmSystem.Uger, googleEnv, ugerEnv, Local)
    doTest(DrmSystem.Uger, Local, Local, Local)
    doTest(DrmSystem.Uger, googleEnv, googleEnv, googleEnv)
    doTest(DrmSystem.Uger, ugerEnv, ugerEnv, ugerEnv)
    doTest(DrmSystem.Uger, googleEnv, ugerEnv, ugerEnv)
    doTest(DrmSystem.Uger, ugerEnv, Local, Local)
    doTest(DrmSystem.Uger, Local, ugerEnv, Local)
    
    doTest(DrmSystem.Lsf, Local, lsfEnv, googleEnv)
    doTest(DrmSystem.Lsf, googleEnv, lsfEnv, Local)
    doTest(DrmSystem.Lsf, Local, Local, Local)
    doTest(DrmSystem.Lsf, googleEnv, googleEnv, googleEnv)
    doTest(DrmSystem.Lsf, lsfEnv, lsfEnv, lsfEnv)
    doTest(DrmSystem.Lsf, googleEnv, lsfEnv, lsfEnv)
    doTest(DrmSystem.Lsf, lsfEnv, Local, Local)
    doTest(DrmSystem.Lsf, Local, lsfEnv, Local)
  }
  
  private def envFn[A](env: Environment)(block: => A)(implicit context: LoamScriptContext): A = env match {
    case Environment.Local => LoamPredef.local(block)
    case Environment.Google(_) => LoamPredef.google(block)
    case Environment.Uger(settings) => {
      LoamPredef.ugerWith(settings.cores.value, settings.memoryPerCore.gb, settings.maxRunTime.hours)(block)
    }
    case Environment.Lsf(settings) => {
      LoamPredef.drmWith(settings.cores.value, settings.memoryPerCore.gb, settings.maxRunTime.hours)(block)
    }
  }
}
