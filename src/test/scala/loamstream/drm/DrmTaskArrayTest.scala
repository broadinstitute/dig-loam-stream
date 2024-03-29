package loamstream.drm

import java.nio.file.Path

import scala.collection.immutable.Seq

import org.scalatest.FunSuite

import loamstream.conf.DrmConfig
import loamstream.conf.ExecutionConfig
import loamstream.conf.LsfConfig
import loamstream.conf.UgerConfig
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.drm.uger.UgerPathBuilder
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript.Implicits.BashPath
import loamstream.util.Files
import loamstream.TestHelpers
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.drm.uger.UgerDefaults
import loamstream.drm.lsf.LsfScriptBuilderParams
import loamstream.conf.Locations
import loamstream.model.execute.LocalSettings
import loamstream.model.jobs.LJob

/**
 * @author clint
 * Nov 22, 2017
 */
object DrmTaskArrayTest {
  import loamstream.TestHelpers.path

  def job(commandLine: String): CommandLineJob = CommandLineJob(commandLine, path("."), LocalSettings)

  private val workDir = path("/foo/bar/baz").toAbsolutePath

  private val jobOutputDir = path("/path/to/logs").toAbsolutePath

  private val ugerConfig = UgerConfig(maxNumJobsPerTaskArray = 42)
  private val lsfConfig = LsfConfig(maxNumJobsPerTaskArray = 42)

  private val executionConfig = ExecutionConfig(maxRunsPerJob = 42)

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
    override def drmSystem: DrmSystem = ???
  }
}

final class DrmTaskArrayTest extends FunSuite {
  import DrmTaskArrayTest._
  import loamstream.TestHelpers.path
  import loamstream.TestHelpers.defaultUgerSettings

  private val ugerPathBuilder = new UgerPathBuilder(UgerScriptBuilderParams(TestHelpers.configWithUger.ugerConfig.get))
  
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
      
      val drmConfig = ugerConfig.copy(workDir = workDir)
      
      val jobOracle = TestHelpers.InDirJobOracle(workDir)
      
      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(
            executionConfig, 
            jobOracle,
            defaultUgerSettings, 
            drmConfig, 
            pathBuilder, 
            jobs, 
            expectedJobName)
      }
  
      assert(taskArray.drmJobs.forall(_.drmSettings === defaultUgerSettings))
      assert(taskArray.drmConfig === drmConfig)
  
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
          s"foo${workDir.render}/${expectedJobName}/bar.stdout",
          s"foo${workDir.render}/${expectedJobName}/bar.stderr")
    }
    
    {
      val pathBuilder: PathBuilder = {
        MockPathBuilder(pathTemplatePrefix = "", MockScriptBuilderParams(drmIndexVarExpr = "bar"))
      }
    
      doTest(
          pathBuilder, 
          s"${workDir.render}/${expectedJobName}/bar.stdout",
          s"${workDir.render}/${expectedJobName}/bar.stderr")
    }
  }

  test("size") {
    def doTest(pathBuilder: PathBuilder): Unit = {
      import TestHelpers.defaultUgerSettings
      
      val jobOracle = TestHelpers.DummyJobOracle
      
      val taskArray = {
        DrmTaskArray.fromCommandLineJobs(executionConfig, jobOracle, defaultUgerSettings, ugerConfig, pathBuilder, jobs)
      }
  
      assert(taskArray.size === 3)
      assert(taskArray.size === jobs.size)
    }
    
    doTest(ugerPathBuilder)
    doTest(LsfPathBuilder)
  }

  test("scriptContents") {
    def doTest(drmConfig: DrmConfig): Unit = {
      //Expedient
      val pathBuilder = if(drmConfig.isUgerConfig) ugerPathBuilder else LsfPathBuilder
      val scriptBuilderParams = drmConfig.scriptBuilderParams
      
      import TestHelpers.defaultUgerSettings
      
      TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
       
        val jobOracle = TestHelpers.InDirJobOracle(workDir)
        
        val taskArray = DrmTaskArray.fromCommandLineJobs(
            executionConfig, 
            jobOracle, 
            defaultUgerSettings, 
            drmConfig, 
            pathBuilder, 
            jobs)
  
        assert(taskArray.scriptContents === (new ScriptBuilder(scriptBuilderParams)).buildFrom(taskArray))
      }
    }
    
    doTest(ugerConfig)
    doTest(lsfConfig)
  }

  test("drmScriptFile/writeDrmScriptFile") {
    def doTest(drmConfig: DrmConfig): Unit = {
      //Expedient
      val pathBuilder = if(drmConfig.isUgerConfig) ugerPathBuilder else LsfPathBuilder
      val scriptBuilderParams = drmConfig.scriptBuilderParams

      import TestHelpers.defaultUgerSettings
      
      val jobOracle = TestHelpers.InDirJobOracle(drmConfig.workDir)

      val arrayName = "blahblahblah"
      
      val taskArray = DrmTaskArray.fromCommandLineJobs(
          executionConfig, 
          jobOracle, 
          defaultUgerSettings, 
          drmConfig, 
          pathBuilder, 
          jobs,
          jobName = arrayName)
  
      assert(taskArray.scriptContents === (new ScriptBuilder(scriptBuilderParams)).buildFrom(taskArray))
  
      assert(taskArray.drmScriptFile.getParent === drmConfig.workDir.resolve(arrayName))
  
      assert(Files.readFrom(taskArray.drmScriptFile) === taskArray.scriptContents)
      
      def scriptFileFor(j: LJob): Path = jobOracle.dirFor(j).resolve("drm-script.sh")
      
      assert(Files.readFrom(scriptFileFor(j0)) === taskArray.scriptContents)
      assert(Files.readFrom(scriptFileFor(j1)) === taskArray.scriptContents)
      assert(Files.readFrom(scriptFileFor(j2)) === taskArray.scriptContents)
    }
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      doTest(UgerConfig(workDir = workDir, maxNumJobsPerTaskArray = 99))
      //Run the test again, to make sure script files are overwritten without errors 
      doTest(UgerConfig(workDir = workDir, maxNumJobsPerTaskArray = 99))
    }
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      doTest(LsfConfig(workDir = workDir, maxNumJobsPerTaskArray = 99))
      //Run the test again, to make sure script files are overwritten without errors
      doTest(LsfConfig(workDir = workDir, maxNumJobsPerTaskArray = 99))
    }
  }
  
  test("TaskIndexingStrategy") {
    import DrmTaskArray.TaskIndexingStrategy
    import DrmSystem._

    assert(TaskIndexingStrategy.forDrmSystem(Uger).toIndex(1) === 2)
    assert(TaskIndexingStrategy.forDrmSystem(Uger)(1) === 2)
    
    assert(TaskIndexingStrategy.forDrmSystem(Lsf).toIndex(1) === 2)
    assert(TaskIndexingStrategy.forDrmSystem(Lsf)(1) === 2)
    
    assert(TaskIndexingStrategy.forDrmSystem(Slurm).toIndex(1) === 2)
    assert(TaskIndexingStrategy.forDrmSystem(Slurm)(1) === 2)
  }
  
  test("TaskIndexingStrategy.forDrmSystem") {
    import DrmTaskArray.TaskIndexingStrategy
    import TaskIndexingStrategy.forDrmSystem
    import DrmSystem._
    
    assert(forDrmSystem(DrmSystem.Uger) === TaskIndexingStrategy.PlusOne)
    assert(forDrmSystem(DrmSystem.Lsf) === TaskIndexingStrategy.PlusOne)
    assert(forDrmSystem(DrmSystem.Slurm) === TaskIndexingStrategy.PlusOne)
  }
}
