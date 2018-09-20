package loamstream.drm

import org.scalatest.FunSuite
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript.Implicits._
import org.scalatest.Finders
import scala.collection.Seq
import loamstream.TestHelpers
import loamstream.conf.UgerConfig
import loamstream.conf.DrmConfig
import loamstream.drm.uger.UgerPathBuilder
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.model.execute.DrmSettings

/**
 * @author clint
 * Nov 22, 2017
 */
final class DrmJobWrapperTest extends FunSuite {
  import TestHelpers.path
  import DrmTaskArrayTest._

  private def wrapper(commandLineJob: CommandLineJob, ugerIndex: Int, pathBuilder: PathBuilder): DrmJobWrapper = {
    DrmJobWrapper(executionConfig, TestHelpers.defaultUgerSettings, pathBuilder, commandLineJob, ugerIndex)
  }

  private val ugerSettings = TestHelpers.defaultUgerSettings
  
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
    val ugerSettingsWITHContainer = ugerSettingsNoContainer.copy(dockerParams = Option(DockerParams(imageName)))
    
    val lsfSettingsNoContainer = TestHelpers.defaultLsfSettings
    val lsfSettingsWITHContainer = lsfSettingsNoContainer.copy(dockerParams = Option(DockerParams(imageName)))
    
    doTest(UgerPathBuilder, ugerSettingsNoContainer, "")
    doTest(UgerPathBuilder, ugerSettingsWITHContainer, "singularity exec fooImage.simg ")
    
    doTest(LsfPathBuilder, lsfSettingsNoContainer, "")
    doTest(LsfPathBuilder, lsfSettingsWITHContainer, "singularity exec fooImage.simg ")
  }
}
