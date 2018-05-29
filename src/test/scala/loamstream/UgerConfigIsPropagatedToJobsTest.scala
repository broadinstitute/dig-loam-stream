package loamstream

import org.scalatest.FunSuite

import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef
import loamstream.model.execute.Environment
import loamstream.model.execute.DrmSettings
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.loam.LoamCmdTool
import loamstream.drm.uger.UgerDefaults
import loamstream.drm.DrmSystem


/**
 * @author clint
 * Oct 11, 2017
 */
final class UgerConfigIsPropagatedToJobsTest extends FunSuite {
  test("uger config is propagated to jobs") {
    def doTest(drmSystem: DrmSystem): Unit = {
      val graph = TestHelpers.makeGraph(drmSystem) { implicit context =>
        import LoamPredef._
        import LoamCmdTool._
      
        val a = store("a.txt").asInput
        val b = store("b.txt")
      
        drmWith(cores = 4, mem = 16, maxRunTime = 5) {
          cmd"cp $a $b".in(a).out(b)
        }
      }
      
      val executable = LoamEngine.toExecutable(graph)
  
      val jobs = executable.jobs
      
      assert(jobs.size === 1)
      
      val queue = drmSystem match {
        case DrmSystem.Uger => Option(UgerDefaults.queue)
        case DrmSystem.Lsf => None
      }
      
      val settings = DrmSettings(Cpus(4), Memory.inGb(16), CpuTime.inHours(5), queue)
      
      val expectedEnv = drmSystem match {
        case DrmSystem.Uger => Environment.Uger(settings)
        case DrmSystem.Lsf => Environment.Lsf(settings)
      }
      
      assert(jobs.head.executionEnvironment === expectedEnv)
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
  
  test("uger config is propagated to jobs - defaults") {
    def doTest(drmSystem: DrmSystem): Unit = {
      val graph = TestHelpers.makeGraph(drmSystem) { implicit context =>
        import LoamPredef._
        import LoamCmdTool._
      
        val a = store("a.txt").asInput
        val b = store("b.txt")
      
        drmWith() {
          cmd"cp $a $b".in(a).out(b)
        }
      }
      
      val executable = LoamEngine.toExecutable(graph)
  
      val jobs = executable.jobs
      
      assert(jobs.size === 1)
      
      val expectedEnv = drmSystem match {
        case DrmSystem.Uger => Environment.Uger(TestHelpers.defaultUgerSettings)
        case DrmSystem.Lsf => Environment.Lsf(TestHelpers.defaultLsfSettings)
      }
      
      assert(jobs.head.executionEnvironment === expectedEnv)
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
  
  test("uger config is propagated to jobs - defaults (no arg)") {
    def doTest(drmSystem: DrmSystem): Unit = {
      val graph = TestHelpers.makeGraph(drmSystem) { implicit context =>
        import LoamPredef._
        import LoamCmdTool._
      
        val a = store("a.txt").asInput
        val b = store("b.txt")
      
        drm {
          cmd"cp $a $b".in(a).out(b)
        }
      }
      
      val executable = LoamEngine.toExecutable(graph)
  
      val jobs = executable.jobs
      
      assert(jobs.size === 1)
      
      val expectedEnv = drmSystem match {
        case DrmSystem.Uger => Environment.Uger(TestHelpers.defaultUgerSettings)
        case DrmSystem.Lsf => Environment.Lsf(TestHelpers.defaultLsfSettings)
      }
      
      assert(jobs.head.executionEnvironment === expectedEnv)
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
}
