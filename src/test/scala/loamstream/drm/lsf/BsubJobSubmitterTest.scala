package loamstream.drm.lsf

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.ExecutionConfig
import loamstream.conf.LsfConfig
import loamstream.drm.ContainerParams
import loamstream.drm.DrmSubmissionResult
import loamstream.drm.DrmTaskArray
import loamstream.drm.Queue
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.RunResults
import loamstream.drm.DrmTaskId
import loamstream.util.Observables
import monix.execution.Scheduler
import loamstream.util.CommandInvoker

/**
 * @author clint
 * May 15, 2018
 */
final class BsubJobSubmitterTest extends FunSuite {
  import loamstream.TestHelpers.path
  import loamstream.TestHelpers.waitForT
  import Observables.Implicits.ObservableOps
  import Scheduler.Implicits.global
  
  test("submitJobs - happy path") {
    val submissionFn: ((DrmSettings, DrmTaskArray)) => Try[RunResults] = { case (drmSettings, taskArray) =>
      Success(RunResults("mock", 0, makeBsubOutput("1234"), Seq.empty))
    }
    
    val submitter = new BsubJobSubmitter(
      new CommandInvoker.Sync.JustOnce(
        "bsub", 
        submissionFn,
        isSuccess = RunResults.SuccessPredicate.zeroIsSuccess))
    
    val result = waitForT(submitter.submitJobs(settings, taskArray).firstL)
    
    assert(result.isSuccess)
    
    val DrmSubmissionResult.SubmissionSuccess(jobsToIds) = result
    
    val expected = Map(
        DrmTaskId("1234", 1) -> taskArray.drmJobs(0),
        DrmTaskId("1234", 2) -> taskArray.drmJobs(1))
        
    assert(jobsToIds === expected)
  }
  
  test("submitJobs - submission failure") {
    val submissionFn: ((DrmSettings, DrmTaskArray)) => Try[RunResults] = { case (drmSettings, taskArray) =>
      Success(RunResults("mock", 42, Seq.empty, Seq.empty))
    }
    
    val submitter = new BsubJobSubmitter(
      new CommandInvoker.Sync.JustOnce(
        "bsub", 
        submissionFn,
        isSuccess = RunResults.SuccessPredicate.zeroIsSuccess))
    
    val result = waitForT(submitter.submitJobs(settings, taskArray).firstL)
    
    assert(result.isFailure)
  }
  
  test("submitJobs - something threw") {
    val submissionFn: ((DrmSettings, DrmTaskArray)) => Try[RunResults] = { case (drmSettings, taskArray) =>
      Failure(new Exception)
    }
    
    val submitter = new BsubJobSubmitter(
      new CommandInvoker.Sync.JustOnce(
        "bsub", 
        submissionFn,
        isSuccess = RunResults.SuccessPredicate.zeroIsSuccess))
    
    val result = waitForT(submitter.submitJobs(settings, taskArray).firstL)
    
    assert(result.isFailure)
  }
  
  test("failure") {
    import BsubJobSubmitter.failure
    
    val msg = "asdasdasdasdasdasd"
    
    val submissionResult = failure(msg)
    
    assert(submissionResult.isFailure === true)
    assert(submissionResult.failed.get.getMessage === msg)
  }
  
  test("extractJobId") {
    import BsubJobSubmitter.extractJobId
    
    assert(extractJobId(actualBsubOutput) === Some("2738574"))
    
    assert(extractJobId(multilineBsubOutput) === Some("2738574"))
    
    val withLeadingAndTrailingJunk = Seq("", "   ") ++ actualBsubOutput ++ Seq(" ")
    
    assert(extractJobId(withLeadingAndTrailingJunk) === Some("2738574"))
    
    val withUnTrimmedStrings = multilineBsubOutput.map(s => s"  $s   ")
    
    assert(extractJobId(withUnTrimmedStrings) === Some("2738574"))
  }
  
  test("makeTokens") {
    def doTest(settings: LsfDrmSettings): Unit = {
      import BsubJobSubmitter.makeTokens
      
      val executableName = "/definitely/not/the/default"
      
      val lsfConfig = TestHelpers.config.lsfConfig.get    
      
      val tokens = makeTokens(executableName, lsfConfig, taskArray, settings)
      
      val expectedTokens = Seq(
          executableName,
          "-q",
          queue.name,
          "-W",
          "3:0",
          "-M",
          "7000",
          "-R",
          s"rusage[mem=7000]",
          "-n",
          "42",
          "-R",
          "span[hosts=1]",
          "-J",
          s"${taskArray.drmJobName}[1-2]",
          "-oo",
          taskArray.stdOutPathTemplate,
          "-eo",
          taskArray.stdErrPathTemplate)
          
      assert(tokens === expectedTokens)
    }
    
    doTest(settings)
    doTest(settingsWithoutContainer)
  }
  
  private def makeBsubOutput(baseJobId: String): Seq[String] = Seq(
    s"Job <$baseJobId> is submitted to default queue <research-rh7>."
  )
  
  private val actualBsubOutput: Seq[String] = Seq(
    "Job <2738574> is submitted to default queue <research-rh7>."
  )
  
  //This kind of output shouldn't ever happen, but it lets us test that the first job id is the one that's picked.
  private val multilineBsubOutput: Seq[String] = Seq(
    "Job <2738574> is submitted to default queue <research-rh7>.",
    "Job <1234567> is submitted to default queue <research-rh7>."
  )
  
  private val queue = Queue("fooQueue")
  
  private val settings = LsfDrmSettings(
      cores = Cpus(42),
      memoryPerCore = Memory.inGb(7),
      maxRunTime = CpuTime.inHours(3),
      queue = Some(queue),
      containerParams = Some(ContainerParams(imageName = "library/foo:1.2.3", "")))
  
  private lazy val settingsWithoutContainer = settings.copy(containerParams = None)
          
  private val drmConfig = LsfConfig()
    
  private def commandLineJob(commandLine: String) = CommandLineJob(
      commandLineString = commandLine,
      workDir = path("."),
      initialSettings = settings)
    
  private val job0 = commandLineJob("asdf")
  private val job1 = commandLineJob("echo 42")
    
  private val taskArray = DrmTaskArray.fromCommandLineJobs(
      executionConfig = ExecutionConfig.default,
      jobOracle = TestHelpers.DummyJobOracle,
      drmSettings = settings,
      drmConfig = drmConfig,
      pathBuilder = LsfPathBuilder,
      jobs = Seq(job0, job1))
}
