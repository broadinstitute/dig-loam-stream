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
import loamstream.drm.SessionSource
import java.nio.file.Paths

/**
 * @author clint
 * May 11, 2018
 */
final class QsubTest extends FunSuite {
  import QsubTest.MockSessionSource
  
  test("makeTokens") {
    def doTest(containerParams: Option[ContainerParams], expectedOsPart: Seq[String]): Unit = {
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

      val drmScriptFile = Paths.get("target/foo")
      
      def getNativeSpec(uc: UgerConfig) = {
        val params = Qsub.Params(uc, ugerSettings, 99, drmScriptFile)
        
        Qsub.makeTokens(MockSessionSource("lalala"), "bin/foo", params)
      }
      
      //default native spec
      {
        assert(ugerConfig.staticJobSubmissionParams === UgerDefaults.staticJobSubmissionParams)
        
        val actualNativeSpec = getNativeSpec(ugerConfig)
       
        val expected = Seq(
          "bin/foo",
          "-cwd",
          "-shell",
          "y",
          "-b",
          "n",
          "-si",
          "lalala",
          "-t",
          "1-99",
          "-binding",
          "linear:42",
          "-pe",
          "smp",
          "42", 
          "-q",
          "broad",
          "-l",
          "h_rt=33:0:0,h_vmem=17G") ++ expectedOsPart :+ drmScriptFile.toAbsolutePath.toString
        
        assert(actualNativeSpec === expected)
      }
      //non-default native spec
      {
        val nonDefaultUgerConfig = ugerConfig.copy(staticJobSubmissionParams = "foo bar baz")
        
        val actualNativeSpec = getNativeSpec(nonDefaultUgerConfig)
       
        val expected = Seq(
          "bin/foo",
          "foo",
          "bar",
          "baz",
          "-si",
          "lalala",
          "-t",
          "1-99",
          "-binding",
          "linear:42",
          "-pe",
          "smp",
          "42",
          "-q",
          "broad",
          "-l",
          "h_rt=33:0:0,h_vmem=17G") ++ expectedOsPart :+ drmScriptFile.toAbsolutePath.toString
        
        assert(actualNativeSpec === expected)
      }
    }

    doTest(None, Nil)
    doTest(Some(ContainerParams("docker://library/foo:1.2.3")), Seq("-l", "os=RedHat7"))
  }
}

object QsubTest {
  private final case class MockSessionSource(getSession: String) extends SessionSource {
    override def stop(): Unit = ()
  }
}
