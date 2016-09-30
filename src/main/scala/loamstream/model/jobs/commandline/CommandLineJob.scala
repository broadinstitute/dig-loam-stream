package loamstream.model.jobs.commandline

import scala.concurrent.{ ExecutionContext, Future }
import scala.sys.process.{ ProcessBuilder, ProcessLogger }

import loamstream.model.jobs.JobState

import loamstream.model.jobs.LJob
import loamstream.util.Futures

/**
  * LoamStream
  * Created by oliverr on 6/17/2016.
  */

/** A job based on a command line definition */
trait CommandLineJob extends LJob {
  def processBuilder: ProcessBuilder

  def commandLineString: String

  def logger: ProcessLogger = CommandLineJob.noOpProcessLogger

  def exitValueCheck: Int => Boolean
  
  def exitValueIsOk(exitValue: Int): Boolean = exitValueCheck(exitValue)

  override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = {
    Futures.runBlocking {
      trace(s"RUNNING: $commandLineString")
      val exitValue = processBuilder.run(logger).exitValue
  
      if (exitValueIsOk(exitValue)) {
        trace(s"SUCCEEDED: $commandLineString")
      } else {
        trace(s"FAILED: $commandLineString")
      }
      
      JobState.CommandResult(exitValue)
    }.recover {
      case exception: Exception => JobState.FailedWithException(exception)
    }
  }

  override def toString: String = commandLineString
}

object CommandLineJob {

  val mustBeZero: Int => Boolean = _ == 0
  val acceptAll : Int => Boolean = i => true
  
  val defaultExitValueChecker = mustBeZero

  val noOpProcessLogger = ProcessLogger(line => ())

}
