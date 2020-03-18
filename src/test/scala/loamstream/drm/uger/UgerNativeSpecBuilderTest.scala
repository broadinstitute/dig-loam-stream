package loamstream.drm.uger

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.UgerConfig
import loamstream.model.execute.DrmSettings
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.execute.UgerDrmSettings
import loamstream.drm.DrmSystem
import loamstream.drm.ContainerParams
import loamstream.drm.ContainerParams

/**
 * @author clint
 * May 11, 2018
 */
final class UgerNativeSpecBuilderTest extends FunSuite {
  test("nativeSpec") {
    def doTest(containerParams: Option[ContainerParams], expectedOsPart: String): Unit = {
      import TestHelpers.path
      
      val ugerConfig = UgerConfig(maxNumJobsPerTaskArray = 41)
          
      val ugerSettings = UgerDrmSettings(
          cores = Cpus(42),
          memoryPerCore = Memory.inGb(17),
          maxRunTime = CpuTime.inHours(33),
          queue = Option(UgerDefaults.queue),
          containerParams = containerParams)
          
      assert(ugerSettings.cores !== UgerDefaults.cores)
      assert(ugerSettings.memoryPerCore !== UgerDefaults.memoryPerCore)
      assert(ugerSettings.maxRunTime !== UgerDefaults.maxRunTime)

      def getNativeSpec(uc: UgerConfig) = UgerNativeSpecBuilder(uc).toNativeSpec(ugerSettings)
      
      //default native spec
      {
        assert(ugerConfig.staticJobSubmissionParams === UgerDefaults.staticJobSubmissionParams)
        
        val actualNativeSpec = getNativeSpec(ugerConfig)
       
        val expected = {
          s"-cwd -shell y -b n -binding linear:42 -pe smp 42 -q broad -l h_rt=33:0:0,h_vmem=17G $expectedOsPart"
        }
        
        assert(actualNativeSpec === expected)
      }
      //non-default native spec
      {
        val nonDefaultUgerConfig = ugerConfig.copy(staticJobSubmissionParams = "foo bar baz")
        
        val actualNativeSpec = getNativeSpec(nonDefaultUgerConfig)
       
        val expected = {
          s"foo bar baz -binding linear:42 -pe smp 42 -q broad -l h_rt=33:0:0,h_vmem=17G $expectedOsPart"
        }
        
        assert(actualNativeSpec === expected)
      }
    }

    doTest(None, "")
    doTest(Some(ContainerParams("docker://library/foo:1.2.3")), "-l os=RedHat7")
  }
}
