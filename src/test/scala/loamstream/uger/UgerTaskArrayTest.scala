package loamstream.uger

import org.scalatest.FunSuite
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.TestHelpers
import loamstream.model.execute.Environment
import loamstream.conf.UgerConfig
import org.ggf.drmaa.JobTemplate
import loamstream.conf.ExecutionConfig
import loamstream.util.BashScript.Implicits._
import loamstream.util.Files

/**
 * @author clint
 * Nov 22, 2017
 */
object UgerTaskArrayTest {
  import TestHelpers.path

  def job(commandLine: String): CommandLineJob = CommandLineJob(commandLine, path("."), Environment.Local)

  val workDir = path("/foo/bar/baz").toAbsolutePath

  val jobOutputDir = path("/path/to/logs").toAbsolutePath

  val ugerConfig = UgerConfig(workDir = workDir, maxNumJobs = 42)

  val executionConfig = ExecutionConfig(maxRunsPerJob = 42, jobOutputDir = jobOutputDir)

  val jobs @ Seq(j0, j1, j2) = Seq(job("foo"), job("bar"), job("baz"))
}

final class UgerTaskArrayTest extends FunSuite {
  import TestHelpers.path
  import UgerTaskArrayTest._

  test("makeJobName") {

    val jobName = UgerTaskArray.makeJobName()

    assert(jobName.startsWith("LoamStream-"))
    assert(jobName.size === 47)
  }

  test("ugerStdOutPathTemplate") {
    doPathTemplateTest(
      UgerTaskArray.ugerStdOutPathTemplate,
      s":${workDir.render}/blarg-blahblah.${JobTemplate.PARAMETRIC_INDEX}.stdout")
  }

  test("ugerStdErrPathTemplate") {
    doPathTemplateTest(
      UgerTaskArray.ugerStdErrPathTemplate,
      s":${workDir.render}/blarg-blahblah.${JobTemplate.PARAMETRIC_INDEX}.stderr")
  }

  private def doPathTemplateTest(makeTemplate: (UgerConfig, String) => String, expected: String): Unit = {
    val jobName = "blarg-blahblah"

    val template = makeTemplate(ugerConfig, jobName)

    assert(template === expected)
  }

  test("fromCommandLineJobs") {
    val expectedJobName = UgerTaskArray.makeJobName()
    
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, jobs, expectedJobName)

    assert(taskArray.ugerConfig === ugerConfig)

    assert(taskArray.ugerJobName === expectedJobName)

    assert(taskArray.stdOutPathTemplate === UgerTaskArray.ugerStdOutPathTemplate(ugerConfig, expectedJobName))
    assert(taskArray.stdErrPathTemplate === UgerTaskArray.ugerStdErrPathTemplate(ugerConfig, expectedJobName))

    assert(taskArray.ugerJobs(0).executionConfig === executionConfig)
    assert(taskArray.ugerJobs(1).executionConfig === executionConfig)
    assert(taskArray.ugerJobs(2).executionConfig === executionConfig)

    assert(taskArray.ugerJobs(0).commandLineJob === j0)
    assert(taskArray.ugerJobs(1).commandLineJob === j1)
    assert(taskArray.ugerJobs(2).commandLineJob === j2)

    assert(taskArray.ugerJobs(0).ugerIndex == 1)
    assert(taskArray.ugerJobs(1).ugerIndex == 2)
    assert(taskArray.ugerJobs(2).ugerIndex == 3)

    assert(taskArray.ugerJobs.size === 3)
  }

  test("size") {
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, jobs)

    assert(taskArray.size === 3)
    assert(taskArray.size === jobs.size)
  }

  test("scriptContents") {
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, jobs)

    assert(taskArray.scriptContents === ScriptBuilder.uger.buildFrom(taskArray))
  }

  test("ugerScriptFile/writeUgerScriptFile") {
    val workDir = Files.tempFile("foo").getParent

    val ugerConfig = UgerConfig(workDir = workDir, maxNumJobs = 42)

    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, jobs)

    assert(taskArray.scriptContents === ScriptBuilder.uger.buildFrom(taskArray))

    assert(taskArray.ugerScriptFile.getParent === workDir)

    assert(Files.readFrom(taskArray.ugerScriptFile) === taskArray.scriptContents)
  }
}
