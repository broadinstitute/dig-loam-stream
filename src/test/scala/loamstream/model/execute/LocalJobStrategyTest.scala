package loamstream.model.execute

import java.time.Instant

import scala.util.Success

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.RunData
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.ExitCodes
import loamstream.util.ToFilesProcessLogger
import loamstream.conf.UgerConfig
import loamstream.conf.LsfConfig
import loamstream.googlecloud.ClusterConfig

/**
 * @author clint
 * Jun 13, 2019
 */
final class LocalJobStrategyTest extends FunSuite {
  test("makeRunData") {
    import LocalJobStrategy.makeRunData
    
    def doTest(exitCode: Int, settings: Settings): Unit = {
      TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
        val exitValueAttempt = Success(exitCode) 
        val start = Instant.now
        val end = Instant.now
        val commandLineJob = CommandLineJob(commandLineString = "foo --bar", initialSettings = settings)
        val jobDir = workDir
        val processLogger = ToFilesProcessLogger(workDir.resolve("stdout"), workDir.resolve("stderr"))
        
        val actual = makeRunData(exitValueAttempt, start, end, commandLineJob, jobDir, processLogger)
        
        val expectedStatus = if(ExitCodes.isSuccess(exitCode)) JobStatus.WaitingForOutputs else JobStatus.Failed 
        
        val expected = RunData(
            job = commandLineJob,
            settings = settings,
            jobStatus = expectedStatus, 
            jobResult = Some(CommandResult(exitCode)), 
            resourcesOpt = Some(LocalResources(start, end)), 
            jobDirOpt = Some(jobDir),
            terminationReasonOpt = None)
            
        assert(actual === expected)
      }
    }
    
    doTest(0, LocalSettings)
    doTest(42, LocalSettings)
    
    doTest(0, GoogleSettings("foo", ClusterConfig.default))
    doTest(42, GoogleSettings("foo", ClusterConfig.default))
    
    doTest(0, DrmSettings.fromUgerConfig(UgerConfig()))
    doTest(42, DrmSettings.fromUgerConfig(UgerConfig()))
    
    doTest(0, DrmSettings.fromLsfConfig(LsfConfig()))
    doTest(42, DrmSettings.fromLsfConfig(LsfConfig()))
  }
  
  /*
   * private[execute] def makeRunData(
      exitValueAttempt: Try[Int], 
      start: Instant, 
      end: Instant, 
      commandLineJob: CommandLineJob, 
      jobDir: Path,
      processLogger: ToFilesProcessLogger): RunData = {
    
    val (jobStatus, jobResult) = exitValueAttempt match {
        case Success(exitValue) => (JobStatus.fromExitCode(exitValue), CommandResult(exitValue))
        case Failure(e)         => ExecuterHelpers.statusAndResultFrom(e)
      }
      
    val outputStreams = OutputStreams(processLogger.stdoutPath, processLogger.stderrPath)
    
    RunData(
        job = commandLineJob,
        settings = commandLineJob.initialSettings,
        jobStatus = jobStatus, 
        jobResult = Some(jobResult), 
        resourcesOpt = Some(LocalResources(start, end)), 
        jobDirOpt = Some(jobDir),
        terminationReasonOpt = None)
  }
   */
}
