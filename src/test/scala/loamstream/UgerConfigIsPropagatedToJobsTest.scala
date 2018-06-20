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
import loamstream.drm.lsf.LsfDockerParams
import loamstream.drm.DockerParams
import loamstream.loam.LoamGraph


/**
 * @author clint
 * Oct 11, 2017
 */
final class UgerConfigIsPropagatedToJobsTest extends FunSuite {
  test("uger config is propagated to jobs") {
    
    def makeGraph(drmSystem: DrmSystem): LoamGraph = {
      TestHelpers.makeGraph(drmSystem) { implicit context =>
        import LoamPredef._
        import LoamCmdTool._
      
        val a = store("a.txt").asInput
        val b = store("b.txt")
      
        def declareJobForLsf = drmWith(
            cores = 4, 
            mem = 16, 
            maxRunTime = 5, 
            dockerImage = "library/foo:1.2.3", 
            mountsInContainers = Seq(path("foo/bar"), path("/baz")),
            outputBasePathInContainers = path("/blerg")) {
          
          cmd"cp $a $b".in(a).out(b)
        }
        
        def declareJobForUger = drmWith(cores = 4, mem = 16, maxRunTime = 5) {
          cmd"cp $a $b".in(a).out(b)
        }
        
        drmSystem match {
          case DrmSystem.Uger => {
            intercept[Exception] {
              declareJobForLsf
            }
            
            declareJobForUger
          }
          case DrmSystem.Lsf => declareJobForLsf
        }
      }
    }
    
    def doTest(drmSystem: DrmSystem): Unit = {
      val graph = makeGraph(drmSystem)
      
      val executable = LoamEngine.toExecutable(graph)
  
      val jobs = executable.jobs
      
      assert(jobs.size === 1)
      
      val expectedQueue = drmSystem match {
        case DrmSystem.Uger => Option(UgerDefaults.queue)
        case DrmSystem.Lsf => None
      }

      import TestHelpers.path
      
      val expectedDockerParamsOpt: Option[DockerParams] = drmSystem match {
        case DrmSystem.Uger => None
        case DrmSystem.Lsf => {
          Some(LsfDockerParams("library/foo:1.2.3", Seq(path("foo/bar"), path("/baz")), path("/blerg")))
        }
      }
      
      val expectedSettings = DrmSettings(
          Cpus(4), 
          Memory.inGb(16), 
          CpuTime.inHours(5), 
          expectedQueue, 
          expectedDockerParamsOpt)
      
      val expectedEnv = drmSystem match {
        case DrmSystem.Uger => Environment.Uger(expectedSettings)
        case DrmSystem.Lsf => Environment.Lsf(expectedSettings)
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
