package loamstream.drm

import java.nio.file.Paths

import scala.collection.immutable.Seq

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.DrmConfig
import loamstream.conf.ExecutionConfig
import loamstream.conf.LsfConfig
import loamstream.conf.SingularityConfig
import loamstream.conf.UgerConfig
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.drm.uger.UgerPathBuilder
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript.Implicits.BashPath
import loamstream.drm.slurm.SlurmPathBuilder
import loamstream.model.jobs.commandline.HasCommandLine
import loamstream.conf.SlurmConfig
import loamstream.model.execute.SlurmDrmSettings

/**
 * @author clint
 * Nov 22, 2017
 */
final class DrmJobWrapperTest extends FunSuite {
  import DrmTaskArrayTest._
  import loamstream.TestHelpers.path

  private val ugerSettings = TestHelpers.defaultUgerSettings

  private val ugerPathBuilder = new UgerPathBuilder(UgerScriptBuilderParams(TestHelpers.configWithUger.ugerConfig.get))
  
  private val baseExecutionConfig = ExecutionConfig(maxRunsPerJob = 42)
  private val baseUgerConfig = UgerConfig(maxNumJobsPerTaskArray = 42)
  private val baseLsfConfig = LsfConfig(maxNumJobsPerTaskArray = 42)
  private val baseSlurmConfig = SlurmConfig(maxNumJobsPerTaskArray = 42)
  
  private def wrapCommand(drmJob: DrmJobWrapper, command: String): String = {
    s"${DrmJobWrapper.timePrefix(drmJob.statsFileDestPath)} ${command}"
  }

  test("commandLineInTaskArray - no image") {
    val ugerSettings = TestHelpers.defaultUgerSettings
    val lsfSettings = TestHelpers.defaultLsfSettings
    val slurmSettings = TestHelpers.defaultSlurmSettings

    assert(ugerSettings.containerParams === None)
    assert(lsfSettings.containerParams === None)
    assert(slurmSettings.containerParams === None)

    def doTest(pathBuilder: PathBuilder, drmSettings: DrmSettings): Unit = {
      val drmJob = DrmJobWrapper(baseExecutionConfig, drmSettings, pathBuilder, makeJob("foo"), path("."), 1)

      val expected = wrapCommand(drmJob, "foo")

      assert(drmJob.commandLineInTaskArray === expected)
    }

    doTest(ugerPathBuilder, ugerSettings)
    doTest(LsfPathBuilder, lsfSettings)
    doTest(SlurmPathBuilder, slurmSettings)
  }

  test("commandLineInTaskArray - with image, default singularity settings") {
    val ugerSettings = TestHelpers.defaultUgerSettings.copy(containerParams = Option(ContainerParams("bar", "")))
    val lsfSettings = TestHelpers.defaultLsfSettings.copy(containerParams = Option(ContainerParams("baz", "")))
    val slurmSettings = TestHelpers.defaultSlurmSettings.copy(containerParams = Option(ContainerParams("blerg", "")))

    assert(baseExecutionConfig.singularity == SingularityConfig.default)

    def doTest(pathBuilder: PathBuilder, drmSettings: DrmSettings): Unit = {
      val drmJob = DrmJobWrapper(baseExecutionConfig, drmSettings, pathBuilder, makeJob("foo"), path("."), 1)

      val expected = wrapCommand(drmJob, s"singularity exec ${drmSettings.containerParams.get.imageName} foo")

      assert(drmJob.commandLineInTaskArray === expected)
    }

    doTest(ugerPathBuilder, ugerSettings)
    doTest(LsfPathBuilder, lsfSettings)
    doTest(SlurmPathBuilder, slurmSettings)
  }

  test("commandLineInTaskArray - with image, non-default singularity settings") {
    val ugerSettings = {
      TestHelpers.defaultUgerSettings.copy(containerParams = Option(ContainerParams("bar", "--foo --bar")))
    }

    val lsfSettings = {
      TestHelpers.defaultLsfSettings.copy(containerParams = Option(ContainerParams("baz", "--foo --bar")))
    }

    val slurmSettings = {
      TestHelpers.defaultSlurmSettings.copy(containerParams = Option(ContainerParams("blerg", "--foo --bar")))
    }

    def doTest(pathBuilder: PathBuilder, drmSettings: DrmSettings): Unit = {
      val bar = path("/bar").toAbsolutePath
      val fooBarBat = path("/foo/bar/bat").toAbsolutePath

      val singularityConfig = SingularityConfig("blarg", Seq(bar, fooBarBat), "--foo --bar")

      val executionConfigWithSingularityParams = baseExecutionConfig.copy(singularity = singularityConfig)

      val drmJob = {
        DrmJobWrapper(executionConfigWithSingularityParams, drmSettings, pathBuilder, makeJob("foo"), path("."), 1)
      }

      val expected = {
        val imageName = drmSettings.containerParams.get.imageName
        
        s"blarg exec -B ${bar.render} -B ${fooBarBat.render} --foo --bar ${imageName} foo"
      }

      assert(drmJob.commandLineInTaskArray === wrapCommand(drmJob, expected))
    }

    doTest(ugerPathBuilder, ugerSettings)
    doTest(LsfPathBuilder, lsfSettings)
    doTest(SlurmPathBuilder, slurmSettings)
  }

  private def makeJob(commandLine: String) = CommandLineJob(commandLine, Paths.get("."), LocalSettings)

  test("drmStdOutPath") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      val jobName = DrmTaskArray.makeJobName()

      val testWorkDir = TestHelpers.getWorkDir(getClass.getSimpleName)
      
      val drmConfig = baseUgerConfig.copy(workDir = testWorkDir)
      
      val executionConfig = baseExecutionConfig.copy(loamstreamDir = testWorkDir)
      
      val jobOracle = TestHelpers.InDirJobOracle(testWorkDir)
      
      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(
            executionConfig, 
            jobOracle, 
            ugerSettings, 
            drmConfig, 
            pathBuilder, 
            jobs, 
            jobName)
      }

      val Seq(wrapper0, wrapper1, wrapper2) = taskArray.drmJobs

      val stdOutPath0 = wrapper0.drmStdOutPath(taskArray)
      val stdOutPath1 = wrapper1.drmStdOutPath(taskArray)
      val stdOutPath2 = wrapper2.drmStdOutPath(taskArray)

      import loamstream.util.Paths.Implicits.PathHelpers
      
      val expected0 = (drmConfig.workDir / s"$jobName/1.stdout").toAbsolutePath
      val expected1 = (drmConfig.workDir / s"$jobName/2.stdout").toAbsolutePath
      val expected2 = (drmConfig.workDir / s"$jobName/3.stdout").toAbsolutePath

      assert(stdOutPath0 === expected0)
      assert(stdOutPath1 === expected1)
      assert(stdOutPath2 === expected2)
    }

    doTest(ugerPathBuilder)
    doTest(LsfPathBuilder)
  }

  test("drmStdErrPath") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      val jobName = DrmTaskArray.makeJobName()

      val testWorkDir = TestHelpers.getWorkDir(getClass.getSimpleName)
      
      val executionConfig = baseExecutionConfig.copy(loamstreamDir = testWorkDir)
      
      val drmConfig = baseUgerConfig.copy(workDir = testWorkDir)
      
      val jobOracle = TestHelpers.InDirJobOracle(testWorkDir)
      
      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(
            executionConfig, 
            jobOracle, 
            ugerSettings, 
            drmConfig, 
            pathBuilder, 
            jobs, 
            jobName)
      }

      val Seq(wrapper0, wrapper1, wrapper2) = taskArray.drmJobs

      val stdErrPath0 = wrapper0.drmStdErrPath(taskArray)
      val stdErrPath1 = wrapper1.drmStdErrPath(taskArray)
      val stdErrPath2 = wrapper2.drmStdErrPath(taskArray)

      import loamstream.util.Paths.Implicits.PathHelpers
      
      val expected0 = (drmConfig.workDir / s"$jobName/1.stderr").toAbsolutePath
      val expected1 = (drmConfig.workDir / s"$jobName/2.stderr").toAbsolutePath
      val expected2 = (drmConfig.workDir / s"$jobName/3.stderr").toAbsolutePath

      assert(stdErrPath0 === expected0)
      assert(stdErrPath1 === expected1)
      assert(stdErrPath2 === expected2)
    }

    doTest(ugerPathBuilder)
    doTest(LsfPathBuilder)
  }
  
  test("outputStreams.stdout") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      val testWorkDir = TestHelpers.getWorkDir(getClass.getSimpleName)
      
      val drmConfig = baseUgerConfig.copy(workDir = testWorkDir)
      
      val executionConfig = baseExecutionConfig.copy(loamstreamDir = testWorkDir)
      
      val jobOracle = TestHelpers.InDirJobOracle(testWorkDir)
      
      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(executionConfig, jobOracle, ugerSettings, drmConfig, pathBuilder, Seq(j0))
      }

      val Seq(wrapper0) = taskArray.drmJobs

      val expected = path(s"${jobOracle.dirFor(j0)}/stdout").toAbsolutePath

      assert(wrapper0.outputStreams.stdout === expected)
    }

    doTest(ugerPathBuilder)
    doTest(LsfPathBuilder)
  }

  test("outputStreams.stderr") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      val testWorkDir = TestHelpers.getWorkDir(getClass.getSimpleName)
      
      val drmConfig = baseUgerConfig.copy(workDir = testWorkDir)
      
      val executionConfig = baseExecutionConfig.copy(loamstreamDir = testWorkDir)
      
      val jobOracle = TestHelpers.InDirJobOracle(testWorkDir)
      
      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(executionConfig, jobOracle, ugerSettings, drmConfig, pathBuilder, Seq(j0))
      }

      val Seq(wrapper0) = taskArray.drmJobs

      val expected = path(s"${jobOracle.dirFor(j0)}/stderr").toAbsolutePath

      assert(wrapper0.outputStreams.stderr === expected)
    }

    doTest(ugerPathBuilder)
    doTest(LsfPathBuilder)
  }

  test("commandChunk") {
    def doTest(pathBuilder: PathBuilder, settings: DrmSettings, expectedSingularityPart: String): Unit = {
      val jobName = DrmTaskArray.makeJobName()

      val testWorkDir = TestHelpers.getWorkDir(getClass.getSimpleName)
      
      val drmConfig: DrmConfig = settings match {
        case _: UgerDrmSettings => baseUgerConfig.copy(workDir = testWorkDir)
        case _: LsfDrmSettings => baseLsfConfig.copy(workDir = testWorkDir)
        case _: SlurmDrmSettings => baseSlurmConfig.copy(workDir = testWorkDir)
      }
      
      val executionConfig: ExecutionConfig = baseExecutionConfig.copy(loamstreamDir = testWorkDir)
      
      val jobOracle = TestHelpers.InDirJobOracle(testWorkDir)
      
      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(executionConfig, jobOracle, settings, drmConfig, pathBuilder, Seq(j0), jobName)
      }

      val Seq(wrapper0) = taskArray.drmJobs

      val workDir = drmConfig.workDir
      
      val jobOutputDir = executionConfig.jobDataDir.toAbsolutePath

      import loamstream.util.Paths.Implicits.PathHelpers
      
      // scalastyle:off line.size.limit
      val expected = s"""|jobDir="${jobOracle.dirFor(j0).render}"
                         |mkdir -p $$jobDir
                         |
                         |START="$$(date +%Y-%m-%dT%H:%M:%S)"
                         |
                         |${wrapCommand(wrapper0, s"${expectedSingularityPart}${j0.commandLineString}")}
                         |
                         |LOAMSTREAM_JOB_EXIT_CODE=$$?
                         |
                         |echo "Start: $$START\\nEnd: $$(date +%Y-%m-%dT%H:%M:%S)" >> ${wrapper0.statsFileDestPath}
                         |
                         |origStdoutPath="${(workDir / jobName).render}/1.stdout"
                         |origStderrPath="${(workDir / jobName).render}/1.stderr"
                         |
                         |stdoutDestPath="${(jobOracle.dirFor(j0) / "stdout").render}"
                         |stderrDestPath="${(jobOracle.dirFor(j0) / "stderr").render}"
                         |exitcodeDestPath="${(jobOracle.dirFor(j0) / "exitcode").render}"
                         |
                         |echo $$LOAMSTREAM_JOB_EXIT_CODE > $$exitcodeDestPath
                         |
                         |mv $$origStdoutPath $$stdoutDestPath || echo "Couldn't move DRM std out log $$origStdoutPath; it's likely the job wasn't submitted successfully" > $$stdoutDestPath
                         |mv $$origStderrPath $$stderrDestPath || echo "Couldn't move DRM std err log $$origStderrPath; it's likely the job wasn't submitted successfully" > $$stderrDestPath
                         |
                         |exit $$LOAMSTREAM_JOB_EXIT_CODE
                         |""".stripMargin
      // scalastyle:on line.size.limit

      assert(wrapper0.commandChunk(taskArray) === expected)
    }

    val imageName = "fooImage.simg"

    val ugerSettingsNoContainer = TestHelpers.defaultUgerSettings
    val ugerSettingsWITHContainer = {
      ugerSettingsNoContainer.copy(containerParams = Option(ContainerParams(imageName, "--baz")))
    }

    val lsfSettingsNoContainer = TestHelpers.defaultLsfSettings
    val lsfSettingsWITHContainer = {
      lsfSettingsNoContainer.copy(containerParams = Option(ContainerParams(imageName, "--baz")))
    }

    doTest(ugerPathBuilder, ugerSettingsNoContainer, "")
    doTest(ugerPathBuilder, ugerSettingsWITHContainer, "singularity exec --baz fooImage.simg ")

    doTest(LsfPathBuilder, lsfSettingsNoContainer, "")
    doTest(LsfPathBuilder, lsfSettingsWITHContainer, "singularity exec --baz fooImage.simg ")
  }
}
