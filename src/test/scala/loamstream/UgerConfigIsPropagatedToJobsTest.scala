package loamstream

import org.scalatest.FunSuite
import loamstream.compiler.LoamPredef
import loamstream.loam.ops.StoreType
import loamstream.loam.LoamCmdTool
import loamstream.compiler.LoamEngine
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.Environment
import loamstream.conf.UgerSettings
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime

/**
 * @author clint
 * Oct 11, 2017
 */
final class UgerConfigIsPropagatedToJobsTest extends FunSuite {
  test("uger config is propagated to jobs") {
    val graph = TestHelpers.makeGraph { implicit context =>
      import LoamPredef._
      import StoreType.TXT
      import LoamCmdTool._
    
      val a = store[TXT].at("a.txt").asInput
      val b = store[TXT].at("b.txt")
    
      ugerWith(cores = 4, mem = 16, maxRunTime = 5) {
        cmd"cp $a $b".in(a).out(b)
      }
    }
    
    val executable = LoamEngine.toExecutable(graph)

    val jobs = executable.jobs
    
    assert(jobs.size === 1)
    
    val expectedEnv = Environment.Uger(UgerSettings(Cpus(4), Memory.inGb(16), CpuTime.inHours(5)))
    
    assert(jobs.head.executionEnvironment === expectedEnv)
  }
}
