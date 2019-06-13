package loamstream.model.jobs.commandline

import java.nio.file.{ Files => JFiles }
import java.nio.file.Path

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.sys.process.ProcessBuilder
import scala.sys.process.ProcessLogger
import scala.util.Failure
import scala.util.Success

import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult.CommandInvocationFailure
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.DataHandle
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.TimeUtils
import loamstream.util.BashScript
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.Writer
import loamstream.model.jobs.LocalJob
import loamstream.model.jobs.JobNode
import java.nio.file.Paths
import loamstream.model.execute.Settings

/**
 * LoamStream
 * Created by oliverr on 6/17/2016.
 *
 * A job based on a command line definition
 */
final case class CommandLineJob(
    commandLineString: String,
    workDir: Path = Paths.get("."),
    initialSettings: Settings,
    override val dependencies: Set[JobNode] = Set.empty,
    inputs: Set[DataHandle] = Set.empty,
    outputs: Set[DataHandle] = Set.empty,
    exitValueCheck: Int => Boolean = CommandLineJob.defaultExitValueChecker,
    private val nameOpt: Option[String] = None) extends HasCommandLine with Loggable {

  override def equals(other: Any): Boolean = other match {
    case that: CommandLineJob => this.id == that.id
    case _ => false
  }
  
  override def hashCode: Int = id.hashCode
  
  override def name: String = nameOpt.getOrElse(id.toString)

  def withCommandLineString(newCmd: String): CommandLineJob = copy(commandLineString = newCmd)

  override def workDirOpt: Option[Path] = Some(workDir)

  def exitValueIsOk(exitValue: Int): Boolean = exitValueCheck(exitValue)

  override def toString: String = s"${getClass.getSimpleName}#${id}('${commandLineString}', ...)"
}

object CommandLineJob extends Loggable {

  private val mustBeZero: Int => Boolean = _ == 0

  val defaultExitValueChecker: Int => Boolean = mustBeZero

  object WithCommandLineAndOutputs {
    def unapply(job: LJob): Option[(String, Set[DataHandle])] = job match {
      case clj: CommandLineJob => Some((clj.commandLineString, clj.outputs))
      case _                   => None
    }
  }
}
