package loamstream.loam

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef
import loamstream.model.execute.DrmSettings
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
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.Settings
import loamstream.googlecloud.ClusterConfig

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
    assert(job.initialSettings === LocalSettings)
  }
  
  private def makeGraph(settings: Settings): (LoamScriptContext => Any) => LoamGraph = settings.envType match {
    case EnvironmentType.Uger => TestHelpers.makeGraph(DrmSystem.Uger)(_)
    case EnvironmentType.Lsf => TestHelpers.makeGraph(DrmSystem.Lsf)(_)
    case _ => TestHelpers.makeGraph(_)
  }
  
  private def makeGraph(drmSystem: DrmSystem): (LoamScriptContext => Any) => LoamGraph = {
    TestHelpers.makeGraph(drmSystem)(_)
  }
  
  test("Set EE") {
    def doTest(settings: Settings): Unit = {
      val methodName = settings.toString.toLowerCase
      
      val graph = makeGraph(settings) { implicit context => 
        import LoamCmdTool._
      
        envFn(settings) {
          cmd"foo"
        }
      }
      
      val executable = LoamEngine.toExecutable(graph)
      
      val job = executable.jobs.head
      
      assert(executable.jobs.size === 1)
      assert(commandLineFrom(job) === "foo")  
      assert(job.initialSettings === settings)
    }
    
    doTest(LocalSettings)
    doTest {
      UgerDrmSettings(Cpus(2), Memory.inGb(3), CpuTime.inHours(4), Option(UgerDefaults.queue), None)
    }
    doTest(LsfDrmSettings(Cpus(3), Memory.inGb(4), CpuTime.inHours(5), None, None))
    doTest(GoogleSettings(clusterId, ClusterConfig.default))
  }
  
  test("Multiple EEs") {
    def doTest(drmSystem: DrmSystem, env1: Settings, env2: Settings, env3: Settings): Unit = {
      val methodName1 = env1.envType.toString.toLowerCase
      val methodName2 = env2.envType.toString.toLowerCase
      val methodName3 = env3.envType.toString.toLowerCase
      
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
        
      assert(foo.initialSettings === env1)
      assert(bar.initialSettings === env1)
      
      assert(baz.initialSettings === env2)
      
      assert(blerg.initialSettings === env3)
      assert(zerg.initialSettings === env3)
    }
    
    val ugerSettings = TestHelpers.defaultUgerSettings
    val lsfSettings = TestHelpers.defaultLsfSettings
    val googleSettings = GoogleSettings(clusterId, ClusterConfig.default)
    
    doTest(DrmSystem.Uger, LocalSettings, ugerSettings, googleSettings)
    doTest(DrmSystem.Uger, googleSettings, ugerSettings, LocalSettings)
    doTest(DrmSystem.Uger, LocalSettings, LocalSettings, LocalSettings)
    doTest(DrmSystem.Uger, googleSettings, googleSettings, googleSettings)
    doTest(DrmSystem.Uger, ugerSettings, ugerSettings, ugerSettings)
    doTest(DrmSystem.Uger, googleSettings, ugerSettings, ugerSettings)
    doTest(DrmSystem.Uger, ugerSettings, LocalSettings, LocalSettings)
    doTest(DrmSystem.Uger, LocalSettings, ugerSettings, LocalSettings)
    
    doTest(DrmSystem.Lsf, LocalSettings, lsfSettings, googleSettings)
    doTest(DrmSystem.Lsf, googleSettings, lsfSettings, LocalSettings)
    doTest(DrmSystem.Lsf, LocalSettings, LocalSettings, LocalSettings)
    doTest(DrmSystem.Lsf, googleSettings, googleSettings, googleSettings)
    doTest(DrmSystem.Lsf, lsfSettings, lsfSettings, lsfSettings)
    doTest(DrmSystem.Lsf, googleSettings, lsfSettings, lsfSettings)
    doTest(DrmSystem.Lsf, lsfSettings, LocalSettings, LocalSettings)
    doTest(DrmSystem.Lsf, LocalSettings, lsfSettings, LocalSettings)
  }
  
  private def envFn[A](settings: Settings)(block: => A)(implicit context: LoamScriptContext): A = settings match {
    case LocalSettings => LoamPredef.local(block)
    case GoogleSettings(_, clusterConfig) => LoamPredef.googleWith(clusterConfig)(block)
    case settings: DrmSettings => {
      LoamPredef.drmWith(settings.cores.value, settings.memoryPerCore.gb, settings.maxRunTime.hours)(block)
    }
  }
}
