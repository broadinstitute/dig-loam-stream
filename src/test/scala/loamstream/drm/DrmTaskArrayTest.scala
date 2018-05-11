package loamstream.drm

import org.scalatest.FunSuite
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.Environment
import loamstream.conf.UgerConfig
import loamstream.conf.ExecutionConfig
import loamstream.util.BashScript.Implicits._
import loamstream.util.Files
import scala.collection.Seq
import loamstream.TestHelpers
import org.ggf.drmaa.JobTemplate
import org.ggf.drmaa.JobTemplate
import java.nio.file.Path
import loamstream.uger.UgerPathBuilder
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.conf.DrmConfig
import loamstream.uger.UgerScriptBuilderParams
import loamstream.drm.lsf.LsfScriptBuilderParams
import loamstream.conf.LsfConfig

/**
 * @author clint
 * Nov 22, 2017
 */
object DrmTaskArrayTest {
  import TestHelpers.path

  def job(commandLine: String): CommandLineJob = CommandLineJob(commandLine, path("."), Environment.Local)

  val workDir = path("/foo/bar/baz").toAbsolutePath

  val jobOutputDir = path("/path/to/logs").toAbsolutePath

  val ugerConfig = UgerConfig(workDir = workDir, maxNumJobs = 42)
  val lsfConfig = LsfConfig(workDir = workDir, maxNumJobs = 42)

  val executionConfig = ExecutionConfig(maxRunsPerJob = 42, jobOutputDir = jobOutputDir)

  val jobs @ Seq(j0, j1, j2) = Seq(job("foo"), job("bar"), job("baz"))
  
  final case class MockPathBuilder(
      pathTemplatePrefix: String,
      scriptBuilderParams: ScriptBuilderParams) extends PathBuilder {
    
    override def reifyPathTemplate(template: String, drmIndex: Int): Path = ???
  }
  
  final case class MockScriptBuilderParams(override val drmIndexVarExpr: String) extends ScriptBuilderParams {
    override def preamble: Option[String] = ??? 
    override def indexEnvVarName: String = ???
    override def jobIdEnvVarName: String = ???
  }
}

final class DrmTaskArrayTest extends FunSuite {
  import TestHelpers.path
  import DrmTaskArrayTest._

  test("makeJobName") {

    val jobName = DrmTaskArray.makeJobName()

    assert(jobName.startsWith("LoamStream-"))
    assert(jobName.size === 47)
  }

  test("fromCommandLineJobs") {
    val expectedJobName = DrmTaskArray.makeJobName()
    
    def doTest(
        pathBuilder: PathBuilder,
        expectedStdOutPathTemplate: String,
        expectedStdErrPathTemplate: String): Unit = {
      
      val taskArray = DrmTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, pathBuilder, jobs, expectedJobName)
  
      assert(taskArray.drmConfig === ugerConfig)
  
      assert(taskArray.drmJobName === expectedJobName)
  
      assert(taskArray.stdOutPathTemplate === expectedStdOutPathTemplate)
      assert(taskArray.stdErrPathTemplate === expectedStdErrPathTemplate)
  
      assert(taskArray.drmJobs(0).executionConfig === executionConfig)
      assert(taskArray.drmJobs(1).executionConfig === executionConfig)
      assert(taskArray.drmJobs(2).executionConfig === executionConfig)
  
      assert(taskArray.drmJobs(0).commandLineJob === j0)
      assert(taskArray.drmJobs(1).commandLineJob === j1)
      assert(taskArray.drmJobs(2).commandLineJob === j2)
  
      assert(taskArray.drmJobs(0).drmIndex == 1)
      assert(taskArray.drmJobs(1).drmIndex == 2)
      assert(taskArray.drmJobs(2).drmIndex == 3)
  
      assert(taskArray.drmJobs.size === 3)
    }
    
    {
      val pathBuilder: PathBuilder = {
        MockPathBuilder(pathTemplatePrefix = "foo", MockScriptBuilderParams(drmIndexVarExpr = "bar"))
      }
      
      doTest(
          pathBuilder, 
          s"foo${workDir.render}/${expectedJobName}.bar.stdout",
          s"foo${workDir.render}/${expectedJobName}.bar.stderr")
    }
    
    {
      val pathBuilder: PathBuilder = {
        MockPathBuilder(pathTemplatePrefix = "", MockScriptBuilderParams(drmIndexVarExpr = "bar"))
      }
    
      doTest(
          pathBuilder, 
          s"${workDir.render}/${expectedJobName}.bar.stdout",
          s"${workDir.render}/${expectedJobName}.bar.stderr")
    }
  }

  test("size") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      val taskArray = DrmTaskArray.fromCommandLineJobs(executionConfig, ugerConfig, pathBuilder, jobs)
  
      assert(taskArray.size === 3)
      assert(taskArray.size === jobs.size)
    }
    
    doTest(UgerPathBuilder)
    doTest(LsfPathBuilder)
  }

  test("scriptContents") {
    def doTest(drmConfig: DrmConfig): Unit = {
      //Expedient
      val pathBuilder = if(drmConfig.isUgerConfig) UgerPathBuilder else LsfPathBuilder
      val scriptBuilderParams = drmConfig.scriptBuilderParams
      
      val taskArray = DrmTaskArray.fromCommandLineJobs(executionConfig, drmConfig, pathBuilder, jobs)

      assert(taskArray.scriptContents === (new ScriptBuilder(scriptBuilderParams)).buildFrom(taskArray))
    }
    
    doTest(ugerConfig)
    doTest(lsfConfig)
  }

  test("drmScriptFile/writeDrmScriptFile") {
    def workDir = Files.tempFile("foo").getParent
    
    def doTest(drmConfig: DrmConfig): Unit = {
      //Expedient
      val pathBuilder = if(drmConfig.isUgerConfig) UgerPathBuilder else LsfPathBuilder
      val scriptBuilderParams = drmConfig.scriptBuilderParams

      val taskArray = DrmTaskArray.fromCommandLineJobs(executionConfig, drmConfig, pathBuilder, jobs)
  
      assert(taskArray.scriptContents === (new ScriptBuilder(scriptBuilderParams)).buildFrom(taskArray))
  
      assert(taskArray.drmScriptFile.getParent === drmConfig.workDir)
  
      assert(Files.readFrom(taskArray.drmScriptFile) === taskArray.scriptContents)
    }
    
    doTest(UgerConfig(workDir = workDir, maxNumJobs = 99))
    doTest(LsfConfig(workDir = workDir, maxNumJobs = 99))
  }
}
