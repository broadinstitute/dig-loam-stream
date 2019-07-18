package loamstream.model.execute

import java.nio.file.{Files => JFiles}
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.{JobStatus, LJob, LocalJob, OutputStreams, RunData}
import loamstream.model.jobs.commandline.{CloseableProcessLogger, CommandLineJob}
import loamstream.util.ToFilesProcessLogger
import loamstream.util.{BashScript, CanBeClosed, Futures, Loggable, TimeUtils}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import java.nio.file.Path
import loamstream.util.Processes
import scala.util.Try
import java.time.Instant

/**
 * @author clint
 * Nov 9, 2017
 */
object LocalJobStrategy extends Loggable {
  def canBeRun(j: LJob): Boolean = j match {
    case _: CommandLineJob | _: LocalJob => true
    case _ => false
  }

  def execute(
    job: LJob,
    jobDir: Path,
    processLogger: ToFilesProcessLogger)(implicit context: ExecutionContext): Future[RunData] = {

    require(canBeRun(job), s"Expected job to be one we can run locally, but got $job")
    
    job match {
      case commandLineJob: CommandLineJob => executeCommandLineJob(commandLineJob, jobDir, processLogger)
      case localJob: LocalJob             => executeLocalJob(localJob)
    }
  }

  private def executeLocalJob(localJob: LocalJob)(implicit ctx: ExecutionContext): Future[RunData] = localJob.execute

  private def executeCommandLineJob(
    commandLineJob: CommandLineJob,
    jobDir: Path,
    processLogger: ToFilesProcessLogger)(implicit context: ExecutionContext): Future[RunData] = {
    
    Futures.runBlocking {
      val (exitValueAttempt, (start, end)) = TimeUtils.startAndEndTime {
        trace(s"RUNNING: ${commandLineJob.commandLineString}")

        CanBeClosed.enclosed(processLogger) {
          createWorkDirAndRun(commandLineJob, _)
        }
      }
      
      makeRunData(exitValueAttempt.flatten, start, end, commandLineJob, jobDir, processLogger)
    }
  }
  
  private[execute] def makeRunData(
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
  
  private def createWorkDirAndRun(job: CommandLineJob, processLogger: CloseableProcessLogger): Try[Int] = {
    import scala.sys.process.ProcessBuilder
    
    val commandLineString = job.commandLineString
    
    val exitCodeAttempt = for {
      _ <- Try(JFiles.createDirectories(job.workDir))
      processBuilder = BashScript.fromCommandLineString(commandLineString).processBuilder(job.workDir)
    } yield processBuilder.!(processLogger)
    
    if(isTraceEnabled) {
      exitCodeAttempt.foreach { exitCode =>
        if (job.exitValueIsOk(exitCode)) {
          trace(s"SUCCEEDED: '$commandLineString'")
        } else {
          trace(s"FAILED with $exitCode: '$commandLineString'")
        }
      }
    }

    exitCodeAttempt
  }
}
