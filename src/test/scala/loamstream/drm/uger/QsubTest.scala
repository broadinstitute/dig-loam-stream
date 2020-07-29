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
import TestHelpers.path

/**
 * @author clint
 * May 11, 2018
 */
final class QsubTest extends FunSuite {
  private val drmScriptFile = Paths.get("target/foo")
  
  private val ugerConfig = UgerConfig(maxNumJobsPerTaskArray = 41)
  
  private val nonDefaultUgerConfig = ugerConfig.copy(staticJobSubmissionParams = "foo bar baz")
  
  private def getTokens(uc: UgerConfig, ugerSettings: UgerDrmSettings) = {
    val params = Qsub.Params(uc, ugerSettings, 99, drmScriptFile, "stdoutT", "stderrT")
    
    Qsub.makeTokens(MockSessionSource("lalala"), "bin/foo", params)
  }
  
  private def makeUgerSettings(containerParams: Option[ContainerParams]) = UgerDrmSettings(
      cores = Cpus(42),
      memoryPerCore = Memory.inGb(17),
      maxRunTime = CpuTime.inHours(33),
      queue = Option(UgerDefaults.queue),
      containerParams = containerParams)
  
  test("makeTokens") {
    def doTest(containerParams: Option[ContainerParams], expectedOsPart: Seq[String]): Unit = {
      val ugerSettings = makeUgerSettings(containerParams)
          
      assert(ugerSettings.cores !== UgerDefaults.cores)
      assert(ugerSettings.memoryPerCore !== UgerDefaults.memoryPerCore)
      assert(ugerSettings.maxRunTime !== UgerDefaults.maxRunTime)
      
      doTestWithDefaultJobSubmissionParams(ugerSettings, expectedOsPart)
      
      doTestWithNonDefaultJobSubmissionParams(ugerSettings, expectedOsPart)
    }

    doTest(None, Nil)
    doTest(Some(ContainerParams("docker://library/foo:1.2.3")), Seq("-l", "os=RedHat7"))
  }
  
  private def doTestWithDefaultJobSubmissionParams(
      ugerSettings: UgerDrmSettings, 
      expectedOsPart: Seq[String]): Unit = {
    
    assert(ugerConfig.staticJobSubmissionParams === UgerDefaults.staticJobSubmissionParams)
    
    val actualNativeSpec = getTokens(ugerConfig, ugerSettings)
   
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
      "h_rt=33:0:0,h_vmem=17G",
      "-o",
      "stdoutT",
      "-e",
      "stderrT") ++ expectedOsPart :+ drmScriptFile.toAbsolutePath.toString
    
    assert(actualNativeSpec === expected)
  }
  
  private def doTestWithNonDefaultJobSubmissionParams(
      ugerSettings: UgerDrmSettings, 
      expectedOsPart: Seq[String]): Unit = {
    
    assert(nonDefaultUgerConfig.staticJobSubmissionParams !== UgerDefaults.staticJobSubmissionParams)
    
    val actualQsubTokens = getTokens(nonDefaultUgerConfig, ugerSettings)
       
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
      "h_rt=33:0:0,h_vmem=17G",
      "-o",
      "stdoutT",
      "-e",
      "stderrT") ++ expectedOsPart :+ drmScriptFile.toAbsolutePath.toString
    
    assert(actualQsubTokens === expected)
  }
}
