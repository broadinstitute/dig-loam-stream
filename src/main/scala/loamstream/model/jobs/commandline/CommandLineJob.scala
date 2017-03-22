package loamstream.model.jobs.commandline

import java.nio.file.{Path, Files => JFiles}

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.sys.process.ProcessBuilder
import scala.sys.process.ProcessLogger

import loamstream.model.jobs.JobResult
import loamstream.model.jobs.LJob
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.TimeUtils
import loamstream.model.execute.Resources.LocalResources


/**
  * LoamStream
  * Created by oliverr on 6/17/2016.
  * 
  * A job based on a command line definition 
  */
trait CommandLineJob extends LJob {
  def workDir: Path

  override def workDirOpt: Option[Path] = Some(workDir)

  def processBuilder: ProcessBuilder

  def commandLineString: String

  def logger: ProcessLogger = CommandLineJob.stdErrProcessLogger

  def exitValueCheck: Int => Boolean

  def exitValueIsOk(exitValue: Int): Boolean = exitValueCheck(exitValue)

  override protected def executeSelf(implicit context: ExecutionContext): Future[JobResult] = {
    Futures.runBlocking {
      trace(s"RUNNING: $commandLineString")

      val (exitValue, (start, end)) = TimeUtils.startAndEndTime {
        createWorkDirAndRun()
      }

      val resources = LocalResources(start, end)
      
      JobResult.CommandResult(exitValue, Option(resources))
    }.recover {
      case exception: Exception => JobResult.CommandInvocationFailure(exception)
    }
  }
  
  private def createWorkDirAndRun(): Int = {
    JFiles.createDirectories(workDir)
      
    val exitValue = processBuilder.run(logger).exitValue

    if (exitValueIsOk(exitValue)) {
      trace(s"SUCCEEDED: $commandLineString")
    } else {
      trace(s"FAILED: $commandLineString")
    }
        
    exitValue
  }

  override def toString: String = s"'$commandLineString'"
}

object CommandLineJob extends Loggable {

  val mustBeZero: Int => Boolean = _ == 0
  val acceptAll: Int => Boolean = i => true

  val defaultExitValueChecker = mustBeZero

  val noOpProcessLogger = ProcessLogger(line => ())
  val stdErrProcessLogger = ProcessLogger(line => (), line => info(line))

}
