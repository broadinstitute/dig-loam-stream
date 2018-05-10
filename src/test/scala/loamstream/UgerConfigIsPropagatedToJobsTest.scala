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
import loamstream.uger.UgerDefaults


/**
 * @author clint
 * Oct 11, 2017
 */
final class UgerConfigIsPropagatedToJobsTest extends FunSuite {
  test("uger config is propagated to jobs") {
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
    
      val a = store.at("a.txt").asInput
      val b = store.at("b.txt")
    
      ugerWith(cores = 4, mem = 16, maxRunTime = 5) {
        cmd"cp $a $b".in(a).out(b)
      }
    }
    
    val executable = LoamEngine.toExecutable(graph)

    val jobs = executable.jobs
    
    assert(jobs.size === 1)
    
    val expectedEnv = Environment.Uger(
        DrmSettings(Cpus(4), Memory.inGb(16), CpuTime.inHours(5), Option(UgerDefaults.queue)))
    
    assert(jobs.head.executionEnvironment === expectedEnv)
  }
  
  test("uger config is propagated to jobs - defaults") {
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
    
      val a = store.at("a.txt").asInput
      val b = store.at("b.txt")
    
      ugerWith() {
        cmd"cp $a $b".in(a).out(b)
      }
    }
    
    val executable = LoamEngine.toExecutable(graph)

    val jobs = executable.jobs
    
    assert(jobs.size === 1)
    
    val expectedEnv = Environment.Uger(TestHelpers.defaultUgerSettings)
    
    assert(jobs.head.executionEnvironment === expectedEnv)
  }
  
  test("uger config is propagated to jobs - defaults (no arg)") {
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import LoamCmdTool._
    
      val a = store.at("a.txt").asInput
      val b = store.at("b.txt")
    
      uger {
        cmd"cp $a $b".in(a).out(b)
      }
    }
    
    val executable = LoamEngine.toExecutable(graph)

    val jobs = executable.jobs
    
    assert(jobs.size === 1)
    
    val expectedEnv = Environment.Uger(TestHelpers.defaultUgerSettings)
    
    assert(jobs.head.executionEnvironment === expectedEnv)
  }
}
