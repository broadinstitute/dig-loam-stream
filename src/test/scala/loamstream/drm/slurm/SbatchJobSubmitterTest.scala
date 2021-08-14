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
import loamstream.util.CommandInvoker
import loamstream.util.Tries
import monix.eval.Task
import loamstream.drm.DrmSubmissionResult
import loamstream.util.RunResults
import loamstream.drm.DrmTaskId
import loamstream.drm.DrmJobWrapper

/**
 * @author clint
 * May 25, 2021
 */
final class SbatchJobSubmitterTest extends FunSuite {
  test("submitJobs") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val drmSettings: SlurmDrmSettings = SlurmDrmSettings(
        cores = Cpus(5),
        memoryPerCore = Memory.inGb(6),
        maxRunTime = CpuTime.inHours(7),
        queue = None,
        containerParams = None)

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

      val jobId = 83475634

      val invoker: CommandInvoker.Async[SbatchJobSubmitter.Params] = {
        case (_, _) => Task(RunResults.Completed("sbatch", 0, Seq(s"Submitted batch job ${jobId}"), Nil))
      }

      val submitter = new SbatchJobSubmitter(invoker)

      val obs = submitter.submitJobs(drmSettings, taskArray)

      import monix.execution.Scheduler.Implicits.global

      val result = obs.firstL.runSyncUnsafe().get
    
      //Compare most-relevant fields only
      val expected = Map(
        DrmTaskId(jobId.toString, 1) -> (jobs(0), 1),
        DrmTaskId(jobId.toString, 2) -> (jobs(1), 2),
        DrmTaskId(jobId.toString, 3) -> (jobs(2), 3)
      )

      val actual = result.mapValues { case DrmJobWrapper(_, _, _, job, _, idx) => (job, idx) }

      assert(actual === expected)
    }
  }

  test("submitJobs - running sbatch returns non-zero") {
    val invoker: CommandInvoker.Async[SbatchJobSubmitter.Params] = {
      case (_, _) => Task(RunResults.Completed("sbatch", 42, Nil, Nil))
    }

    val submitter = new SbatchJobSubmitter(invoker)

    val obs = submitter.submitJobs(null, null) //scalastyle.ignore:null

    import monix.execution.Scheduler.Implicits.global

    val result = obs.firstL.runSyncUnsafe()
    
    val thrown = intercept[Exception](result.get)

    assert(thrown.getMessage.contains("SLURM Job submission failure"))
  }

  test("submitJobs - running sbatch results in CouldNotStart") {
    val invoker: CommandInvoker.Async[SbatchJobSubmitter.Params] = {
      case (_, _) => Task(RunResults.CouldNotStart("sbatch", new Exception("blerg")))
    }

    val submitter = new SbatchJobSubmitter(invoker)

    val obs = submitter.submitJobs(null, null) //scalastyle.ignore:null

    import monix.execution.Scheduler.Implicits.global

    val result = obs.firstL.runSyncUnsafe()
    
    val thrown = intercept[Exception](result.get)

    assert(thrown.getMessage.contains("SLURM Job submission failure"))
  }

  test("submitJobs - running sbatch throws") {
    val invoker: CommandInvoker.Async[SbatchJobSubmitter.Params] = {
      case (_, _) => Task.fromTry(Tries.failure("blerg"))
    }

    val submitter = new SbatchJobSubmitter(invoker)

    val obs = submitter.submitJobs(null, null) //scalastyle.ignore:null

    import monix.execution.Scheduler.Implicits.global

    val result = obs.firstL.runSyncUnsafe()
    
    val thrown = intercept[Exception](result.get)

    assert(thrown.getMessage === "blerg")
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
          "-J", "some-job-name[0-3]",
          "-o", s"${Locations.Default.slurmDir}/some-job-name/%a.stdout",
          "-e", s"${Locations.Default.slurmDir}/some-job-name/%a.stderr", 
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