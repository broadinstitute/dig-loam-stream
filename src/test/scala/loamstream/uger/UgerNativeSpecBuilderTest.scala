package loamstream.uger

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.conf.UgerConfig
import loamstream.model.execute.DrmSettings
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus

/**
 * @author clint
 * May 11, 2018
 */
final class UgerNativeSpecBuilderTest extends FunSuite {
  test("nativeSpec") {
    import UgerNativeSpecBuilder.toNativeSpec
    import TestHelpers.path
    
    val bogusPath = path("/foo/bar/baz")
    
    val ugerConfig = UgerConfig(workDir = bogusPath, maxNumJobs = 41)
        
    val ugerSettings = DrmSettings(
        cores = Cpus(42),
        memoryPerCore = Memory.inGb(17),
        maxRunTime = CpuTime.inHours(33),
        queue = Option(UgerDefaults.queue))
        
    assert(ugerSettings.cores !== UgerDefaults.cores)
    assert(ugerSettings.memoryPerCore !== UgerDefaults.memoryPerCore)
    assert(ugerSettings.maxRunTime !== UgerDefaults.maxRunTime)
    
    val actualNativeSpec = toNativeSpec(ugerSettings)
    
    val expected = "-cwd -shell y -b n -binding linear:42 -pe smp 42 -q broad -l h_rt=33:0:0,h_vmem=17g"
    
    assert(actualNativeSpec === expected)
  }
}
