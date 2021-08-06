package loamstream.drm.slurm

import org.scalatest.FunSuite
import loamstream.drm.DrmTaskArray
import loamstream.model.execute.SlurmDrmSettings
import loamstream.drm.ContainerParams
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.conf.ExecutionConfig
import loamstream.TestHelpers
import loamstream.conf.SlurmConfig
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.conf.Locations

/**
 * @author clint
 * May 25, 2021
 */
final class SbatchJobSubmitterTest extends FunSuite {
  test("submitJobs") {
    fail("TODO")
  }

  test("makeTokens") {
    import SbatchJobSubmitter.makeTokens

    def doTest(containerParams: Option[ContainerParams]): Unit = {
      TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
        val drmSettings: SlurmDrmSettings = SlurmDrmSettings(
          cores = Cpus(5),
          memoryPerCore = Memory.inGb(6),
          maxRunTime = CpuTime.inHours(7),
          queue = None,
          containerParams = containerParams)

        val jobs: Seq[CommandLineJob] = (1 to 3).map { i =>
          CommandLineJob(
            commandLineString = s"foo --bar=${i}",
            initialSettings = drmSettings)
        }

        val taskArray = DrmTaskArray.fromCommandLineJobs(
          executionConfig = ExecutionConfig.default,
          jobOracle = TestHelpers.InDirJobOracle(workDir),
          drmSettings = drmSettings,
          drmConfig = SlurmConfig(),
          pathBuilder = SlurmPathBuilder,
          jobs = jobs,
          jobName = "some-job-name")

        val actual = makeTokens(
          actualExecutable = "foo",
          taskArray = taskArray,
          drmSettings = drmSettings)

        val expected: Seq[String] = Seq(
          "foo",
          "--array=0-3",
          "-t", "7:0:0",
          "--mem-per-cpu=6G",
          "--cpus-per-task=5",
          "-J", "some-job-name",
          "-o", s"${Locations.Default.slurmDir}/some-job-name.%a.stdout",
          "-e", s"${Locations.Default.slurmDir}/some-job-name.%a.stderr", 
          taskArray.drmScriptFile.toString
        )

        assert(actual === expected)
      }
    }

    doTest(None)
    doTest(Some(ContainerParams(imageName = "some-image", extraParams = "lalala")))
  }
  
  test("extractJobId") {
    import SbatchJobSubmitter.extractJobId
    
    assert(extractJobId(Nil) === None)
    assert(extractJobId(Seq("asdasdda")) === None)
    assert(extractJobId(Seq("asdasdda", "", "aksldfjlas")) === None)
    
    assert(extractJobId(Seq("Submitted batch job 42")) === Some("42"))
  }
}