package loamstream.drm.lsf

import org.scalatest.FunSuite
import loamstream.conf.DrmConfig
import loamstream.conf.LsfConfig
import loamstream.TestHelpers
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.CpuTime
import loamstream.drm.Queue
import loamstream.model.execute.DrmSettings
import loamstream.model.quantities.Memory
import loamstream.drm.DrmTaskArray
import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.Environment
import loamstream.util.ExitCodes
import loamstream.util.Files
import scala.util.Try
import scala.util.Success
import loamstream.drm.DrmSubmissionResult
import scala.util.Failure

/**
 * @author clint
 * May 15, 2018
 */
final class BsubJobSubmitterTest extends FunSuite {
  import TestHelpers.path
  
  test("submitJobs - happy path") {
    def submissionFn(drmSettings: DrmSettings, taskArray: DrmTaskArray): Try[RunResults] = {
      Success(RunResults("mock", 0, makeBsubOutput("1234"), Seq.empty))
    }
    
    val submitter = new BsubJobSubmitter(submissionFn)
    
    val result = submitter.submitJobs(settings, taskArray)
    
    assert(result.isSuccess)
    
    val DrmSubmissionResult.SubmissionSuccess(jobsToIds) = result
    
    val expected = Map(
        "1234[1]" -> taskArray.drmJobs(0),
        "1234[2]" -> taskArray.drmJobs(1))
        
    assert(jobsToIds === expected)
  }
  
  test("submitJobs - submission failure") {
    def submissionFn(drmSettings: DrmSettings, taskArray: DrmTaskArray): Try[RunResults] = {
      Success(RunResults("mock", 42, Seq.empty, Seq.empty))
    }
    
    val submitter = new BsubJobSubmitter(submissionFn)
    
    val result = submitter.submitJobs(settings, taskArray)
    
    assert(result.isFailure)
  }
  
  test("submitJobs - something threw") {
    def submissionFn(drmSettings: DrmSettings, taskArray: DrmTaskArray): Try[RunResults] = {
      Failure(new Exception)
    }
    
    val submitter = new BsubJobSubmitter(submissionFn)
    
    val result = submitter.submitJobs(settings, taskArray)
    
    assert(result.isFailure)
  }
  
  test("failure") {
    import BsubJobSubmitter.failure
    
    val msg = "asdasdasdasdasdasd"
    
    val submissionResult = failure(msg)
    
    assert(submissionResult.isFailure === true)
    assert(submissionResult.cause.getMessage === msg)
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
    import BsubJobSubmitter.makeTokens
    
    val executableName = "/definitely/not/the/default"
    
    val tokens = makeTokens(executableName, taskArray, settings)
    
    val expectedTokens = Seq(
        executableName,
        "-q",
        queue.name,
        "-W",
        "3:0",
        "-R",
        s"rusage[mem=${7 * 1024}]",
        "-n",
        "42",
        "-R",
        "span[hosts=1]",
        "-J",
        s"${taskArray.drmJobName}[1-2]",
        "-oo",
        s":${taskArray.stdOutPathTemplate}",
        "-eo",
        s":${taskArray.stdErrPathTemplate}")
        
    assert(tokens === expectedTokens)
    
    /*
     * private[lsf] def makeTokens(
      actualExecutable: String, 
      taskArray: DrmTaskArray,
      drmSettings: DrmSettings): Seq[String] = {
    
    val runTimeInHours: Int = drmSettings.maxRunTime.hours.toInt
    val maxRunTimePart = Seq("-W", s"${runTimeInHours}:0")
    
    val memoryPerCoreInMegs = drmSettings.memoryPerCore.mb.toInt
    val memoryPart = Seq("-R", s""""rusage[mem=${memoryPerCoreInMegs}]"""")
    
    val numCores = drmSettings.cores.value
    
    val coresPart = Seq("-n", numCores.toString, "-R", """"span[hosts=1]"""")
    
    val queuePart: Seq[String] = drmSettings.queue.toSeq.flatMap(q => Seq("-q", q.name))
    
    val jobNamePart = Seq("-J", s""""${taskArray.drmJobName}[1-${taskArray.size}]"""")
    
    val stdoutPart = Seq("-oo", s":${taskArray.stdOutPathTemplate}")
    
    val stderrPart = Seq("-eo", s":${taskArray.stdErrPathTemplate}")
    
    actualExecutable +: 
        (queuePart ++ maxRunTimePart ++ memoryPart ++ coresPart ++ jobNamePart ++ stdoutPart ++ stderrPart)
  }
     */
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
  
  private val settings = DrmSettings(
      cores = Cpus(42),
      memoryPerCore = Memory.inGiB(7),
      maxRunTime = CpuTime.inHours(3),
      queue = Some(queue))
    
  private val drmConfig = LsfConfig(workDir = path("/lsf/dir"))
    
  private def commandLineJob(commandLine: String) = CommandLineJob(
      commandLineString = commandLine,
      workDir = path("."),
      executionEnvironment = Environment.Lsf(settings))
    
  private val job0 = commandLineJob("asdf")
  private val job1 = commandLineJob("echo 42")
    
  private val taskArray = DrmTaskArray.fromCommandLineJobs(
      executionConfig = ExecutionConfig.default,
      drmConfig = drmConfig,
      pathBuilder = LsfPathBuilder,
      jobs = Seq(job0, job1))
}
