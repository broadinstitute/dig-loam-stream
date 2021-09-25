package loamstream.model.execute

import java.nio.file.{ Files => JFiles }
import java.nio.file.Path
import java.time.LocalDateTime

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LocalJob
import loamstream.model.jobs.OutputStreams
import loamstream.model.jobs.RunData
import loamstream.model.jobs.commandline.CloseableProcessLogger
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript
import loamstream.util.CanBeClosed
import loamstream.util.Loggable
import loamstream.util.TimeUtils
import loamstream.util.ToFilesProcessLogger
import monix.eval.Task

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
    processLogger: ToFilesProcessLogger): Task[RunData] = {

    require(canBeRun(job), s"Expected job to be one we can run locally, but got $job")
    
    job match {
      case commandLineJob: CommandLineJob => executeCommandLineJob(commandLineJob, jobDir, processLogger)
      case localJob: LocalJob             => executeLocalJob(localJob)
    }
  }

  private def executeLocalJob(localJob: LocalJob): Task[RunData] = localJob.execute

  private def executeCommandLineJob(
    commandLineJob: CommandLineJob,
    jobDir: Path,
    processLogger: ToFilesProcessLogger): Task[RunData] = {
    
    Task {
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
      start: LocalDateTime, 
      end: LocalDateTime, 
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
