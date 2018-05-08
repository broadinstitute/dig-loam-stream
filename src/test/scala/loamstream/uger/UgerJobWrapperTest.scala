package loamstream.uger

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript.Implicits._

/**
 * @author clint
 * Nov 22, 2017
 */
final class UgerJobWrapperTest extends FunSuite {
  import TestHelpers.path
  import UgerTaskArrayTest._

  private def wrapper(commandLineJob: CommandLineJob, ugerIndex: Int): UgerJobWrapper = {
    UgerJobWrapper(executionConfig, commandLineJob, ugerIndex)
  }

  test("ugerStdOutPath") {
    val jobName = UgerTaskArray.makeJobName()
    
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, jobs, jobName)

    val Seq(wrapper0, wrapper1, wrapper2) = taskArray.ugerJobs

    val ugerStdOutPath0 = wrapper0.ugerStdOutPath(taskArray)
    val ugerStdOutPath1 = wrapper1.ugerStdOutPath(taskArray)
    val ugerStdOutPath2 = wrapper2.ugerStdOutPath(taskArray)

    val expected0 = path(s"/foo/bar/baz/$jobName.1.stdout").toAbsolutePath
    val expected1 = path(s"/foo/bar/baz/$jobName.2.stdout").toAbsolutePath
    val expected2 = path(s"/foo/bar/baz/$jobName.3.stdout").toAbsolutePath

    assert(ugerStdOutPath0 === expected0)
    assert(ugerStdOutPath1 === expected1)
    assert(ugerStdOutPath2 === expected2)
  }

  test("ugerStdErrPath") {
    val jobName = UgerTaskArray.makeJobName()
    
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, jobs, jobName)

    val Seq(wrapper0, wrapper1, wrapper2) = taskArray.ugerJobs

    val ugerStdErrPath0 = wrapper0.ugerStdErrPath(taskArray)
    val ugerStdErrPath1 = wrapper1.ugerStdErrPath(taskArray)
    val ugerStdErrPath2 = wrapper2.ugerStdErrPath(taskArray)

    val expected0 = path(s"/foo/bar/baz/$jobName.1.stderr").toAbsolutePath
    val expected1 = path(s"/foo/bar/baz/$jobName.2.stderr").toAbsolutePath
    val expected2 = path(s"/foo/bar/baz/$jobName.3.stderr").toAbsolutePath

    assert(ugerStdErrPath0 === expected0)
    assert(ugerStdErrPath1 === expected1)
    assert(ugerStdErrPath2 === expected2)
  }

  test("outputStreams.stdout") {
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, Seq(j0))

    val Seq(wrapper0) = taskArray.ugerJobs

    val expected = path(s"${executionConfig.jobOutputDir}/${j0.id}.stdout").toAbsolutePath

    assert(wrapper0.outputStreams.stdout === expected)
  }

  test("outputStreams.stderr") {
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, Seq(j0))

    val Seq(wrapper0) = taskArray.ugerJobs

    val expected = path(s"${executionConfig.jobOutputDir}/${j0.id}.stderr").toAbsolutePath

    assert(wrapper0.outputStreams.stderr === expected)
  }

  test("ugerCommandLine") {
    val jobName = UgerTaskArray.makeJobName()
    
    val taskArray = UgerTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, Seq(j0), jobName)

    val Seq(wrapper0) = taskArray.ugerJobs

    // scalastyle:off line.size.limit
    val expected = s"""|${j0.commandLineString}
                       |
                       |LOAMSTREAM_JOB_EXIT_CODE=$$?
                       |
                       |stdoutDestPath="${jobOutputDir.render}/${j0.id}.stdout"
                       |stderrDestPath="${jobOutputDir.render}/${j0.id}.stderr"
                       |
                       |mkdir -p ${jobOutputDir.render}
                       |mv ${workDir.render}/$jobName.1.stdout $$stdoutDestPath || echo "Couldn't move Uger std out log" > $$stdoutDestPath
                       |mv ${workDir.render}/$jobName.1.stderr $$stderrDestPath || echo "Couldn't move Uger std err log" > $$stderrDestPath
                       |
                       |exit $$LOAMSTREAM_JOB_EXIT_CODE
                       |""".stripMargin
    // scalastyle:on line.size.limit

    assert(wrapper0.ugerCommandChunk(taskArray) === expected)
  }
}
