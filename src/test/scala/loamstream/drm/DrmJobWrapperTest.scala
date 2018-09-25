package loamstream.drm

import java.nio.file.Paths

import scala.collection.Seq

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.SingularityConfig
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.drm.uger.UgerPathBuilder
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.Environment
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript.Implicits.BashPath

/**
 * @author clint
 * Nov 22, 2017
 */
final class DrmJobWrapperTest extends FunSuite {
  import DrmTaskArrayTest._
  import loamstream.TestHelpers.path

  private def wrapper(commandLineJob: CommandLineJob, ugerIndex: Int, pathBuilder: PathBuilder): DrmJobWrapper = {
    DrmJobWrapper(executionConfig, TestHelpers.defaultUgerSettings, pathBuilder, commandLineJob, ugerIndex)
  }

  private val ugerSettings = TestHelpers.defaultUgerSettings

  test("commandLineInTaskArray - no image") {
    val ugerSettings = TestHelpers.defaultUgerSettings

    assert(ugerSettings.containerParams === None)

    val lsfSettings = TestHelpers.defaultLsfSettings

    assert(lsfSettings.containerParams === None)

    def doTest(pathBuilder: PathBuilder, drmSettings: DrmSettings): Unit = {
      val drmJob = DrmJobWrapper(executionConfig, drmSettings, pathBuilder, makeJob("foo"), 1)

      assert(drmJob.commandLineInTaskArray === "foo")
    }

    doTest(UgerPathBuilder, ugerSettings)
    doTest(LsfPathBuilder, lsfSettings)
  }

  test("commandLineInTaskArray - with image, default singularity settings") {
    val ugerSettings = TestHelpers.defaultUgerSettings.copy(containerParams = Option(ContainerParams("bar")))

    val lsfSettings = TestHelpers.defaultLsfSettings.copy(containerParams = Option(ContainerParams("baz")))

    assert(executionConfig.singularity == SingularityConfig.default)

    def doTest(pathBuilder: PathBuilder, drmSettings: DrmSettings): Unit = {
      val drmJob = DrmJobWrapper(executionConfig, drmSettings, pathBuilder, makeJob("foo"), 1)

      assert(drmJob.commandLineInTaskArray === s"singularity exec ${drmSettings.containerParams.get.imageName} foo")
    }

    doTest(UgerPathBuilder, ugerSettings)
    doTest(LsfPathBuilder, lsfSettings)
  }

  test("commandLineInTaskArray - with image, non-default singularity settings") {
    val ugerSettings = TestHelpers.defaultUgerSettings.copy(containerParams = Option(ContainerParams("bar")))

    val lsfSettings = TestHelpers.defaultLsfSettings.copy(containerParams = Option(ContainerParams("baz")))

    def doTest(pathBuilder: PathBuilder, drmSettings: DrmSettings): Unit = {
      val bar = path("/bar").toAbsolutePath
      val fooBarBat = path("/foo/bar/bat").toAbsolutePath

      val singularityConfig = SingularityConfig("blarg", Seq(bar, fooBarBat))

      val executionConfigWithSingularityParams = executionConfig.copy(singularity = singularityConfig)

      val drmJob = DrmJobWrapper(executionConfigWithSingularityParams, drmSettings, pathBuilder, makeJob("foo"), 1)

      val expected = {
        s"blarg exec -B ${bar.render} -B ${fooBarBat.render} ${drmSettings.containerParams.get.imageName} foo"
      }

      assert(drmJob.commandLineInTaskArray === expected)
    }

    doTest(UgerPathBuilder, ugerSettings)
    doTest(LsfPathBuilder, lsfSettings)
  }

  private def makeJob(commandLine: String) = CommandLineJob(commandLine, Paths.get("."), Environment.Local)

  test("stdOutPath") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      val jobName = DrmTaskArray.makeJobName()

      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(executionConfig, ugerSettings, ugerConfig, pathBuilder, jobs, jobName)
      }

      val Seq(wrapper0, wrapper1, wrapper2) = taskArray.drmJobs

      val stdOutPath0 = wrapper0.drmStdOutPath(taskArray)
      val stdOutPath1 = wrapper1.drmStdOutPath(taskArray)
      val stdOutPath2 = wrapper2.drmStdOutPath(taskArray)

      val expected0 = path(s"/foo/bar/baz/$jobName.1.stdout").toAbsolutePath
      val expected1 = path(s"/foo/bar/baz/$jobName.2.stdout").toAbsolutePath
      val expected2 = path(s"/foo/bar/baz/$jobName.3.stdout").toAbsolutePath

      assert(stdOutPath0 === expected0)
      assert(stdOutPath1 === expected1)
      assert(stdOutPath2 === expected2)
    }

    doTest(UgerPathBuilder)
    doTest(LsfPathBuilder)
  }

  test("ugerStdErrPath") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      val jobName = DrmTaskArray.makeJobName()

      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(executionConfig, ugerSettings, ugerConfig, pathBuilder, jobs, jobName)
      }

      val Seq(wrapper0, wrapper1, wrapper2) = taskArray.drmJobs

      val stdErrPath0 = wrapper0.drmStdErrPath(taskArray)
      val stdErrPath1 = wrapper1.drmStdErrPath(taskArray)
      val stdErrPath2 = wrapper2.drmStdErrPath(taskArray)

      val expected0 = path(s"/foo/bar/baz/$jobName.1.stderr").toAbsolutePath
      val expected1 = path(s"/foo/bar/baz/$jobName.2.stderr").toAbsolutePath
      val expected2 = path(s"/foo/bar/baz/$jobName.3.stderr").toAbsolutePath

      assert(stdErrPath0 === expected0)
      assert(stdErrPath1 === expected1)
      assert(stdErrPath2 === expected2)
    }

    doTest(UgerPathBuilder)
    doTest(LsfPathBuilder)
  }

  test("outputStreams.stdout") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(executionConfig, ugerSettings, ugerConfig, pathBuilder, Seq(j0))
      }

      val Seq(wrapper0) = taskArray.drmJobs

      val expected = path(s"${executionConfig.jobOutputDir}/${j0.id}.stdout").toAbsolutePath

      assert(wrapper0.outputStreams.stdout === expected)
    }

    doTest(UgerPathBuilder)
    doTest(LsfPathBuilder)
  }

  test("outputStreams.stderr") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(executionConfig, ugerSettings, ugerConfig, pathBuilder, Seq(j0))
      }

      val Seq(wrapper0) = taskArray.drmJobs

      val expected = path(s"${executionConfig.jobOutputDir}/${j0.id}.stderr").toAbsolutePath

      assert(wrapper0.outputStreams.stderr === expected)
    }

    doTest(UgerPathBuilder)
    doTest(LsfPathBuilder)
  }

  test("commandChunk") {
    def doTest(pathBuilder: PathBuilder, settings: DrmSettings, expectedSingularityPart: String): Unit = {
      val jobName = DrmTaskArray.makeJobName()

      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(executionConfig, settings, ugerConfig, pathBuilder, Seq(j0), jobName)
      }

      val Seq(wrapper0) = taskArray.drmJobs

      // scalastyle:off line.size.limit
      val expected = s"""|${expectedSingularityPart}${j0.commandLineString}
                         |
                         |LOAMSTREAM_JOB_EXIT_CODE=$$?
                         |
                         |stdoutDestPath="${jobOutputDir.render}/${j0.id}.stdout"
                         |stderrDestPath="${jobOutputDir.render}/${j0.id}.stderr"
                         |
                         |mkdir -p ${jobOutputDir.render}
                         |mv ${workDir.render}/$jobName.1.stdout $$stdoutDestPath || echo "Couldn't move DRM std out log ${workDir.render}/$jobName.1.stdout; it's likely the job wasn't submitted successfully" > $$stdoutDestPath
                         |mv ${workDir.render}/$jobName.1.stderr $$stderrDestPath || echo "Couldn't move DRM std err log ${workDir.render}/$jobName.1.stderr; it's likely the job wasn't submitted successfully" > $$stderrDestPath
                         |
                         |exit $$LOAMSTREAM_JOB_EXIT_CODE
                         |""".stripMargin
      // scalastyle:on line.size.limit

      assert(wrapper0.commandChunk(taskArray) === expected)
    }

    val imageName = "fooImage.simg"

    val ugerSettingsNoContainer = TestHelpers.defaultUgerSettings
    val ugerSettingsWITHContainer = ugerSettingsNoContainer.copy(containerParams = Option(ContainerParams(imageName)))

    val lsfSettingsNoContainer = TestHelpers.defaultLsfSettings
    val lsfSettingsWITHContainer = lsfSettingsNoContainer.copy(containerParams = Option(ContainerParams(imageName)))

    doTest(UgerPathBuilder, ugerSettingsNoContainer, "")
    doTest(UgerPathBuilder, ugerSettingsWITHContainer, "singularity exec fooImage.simg ")

    doTest(LsfPathBuilder, lsfSettingsNoContainer, "")
    doTest(LsfPathBuilder, lsfSettingsWITHContainer, "singularity exec fooImage.simg ")
  }
}
