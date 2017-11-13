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
import loamstream.model.jobs.Output
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.TimeUtils
import loamstream.model.execute.Environment
import loamstream.util.BashScript
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.Writer
import loamstream.model.jobs.LocalJob

/**
 * LoamStream
 * Created by oliverr on 6/17/2016.
 *
 * A job based on a command line definition
 */
final case class CommandLineJob(
    commandLineString: String,
    workDir: Path,
    executionEnvironment: Environment,
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty,
    exitValueCheck: Int => Boolean = CommandLineJob.defaultExitValueChecker,
    logger: ProcessLogger = CommandLineJob.stdErrProcessLogger,
    private val nameOpt: Option[String] = None) extends LJob with Loggable {

  override def name: String = nameOpt.getOrElse(id)
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  def withCommandLineString(newCmd: String): CommandLineJob = copy(commandLineString = newCmd)

  override def workDirOpt: Option[Path] = Some(workDir)

  def exitValueIsOk(exitValue: Int): Boolean = exitValueCheck(exitValue)

  override def toString: String = s"${getClass.getSimpleName}#${id}('${commandLineString}', ...)"
}

object CommandLineJob extends Loggable {

  private val mustBeZero: Int => Boolean = _ == 0

  val defaultExitValueChecker: Int => Boolean = mustBeZero

  val stdErrProcessLogger: ProcessLogger = ProcessLogger(line => (), line => info(s"(via stderr) $line"))

  //TODO: Close these files somehow!
  def toFilesProcessLogger(stdOutDestination: Path, stdErrDestination: Path): ProcessLogger = {
    def writerFor(p: Path) = new BufferedWriter(new FileWriter(stdOutDestination.toFile))

    val stdout = writerFor(stdOutDestination)
    val stderr = writerFor(stdErrDestination)

    def writeTo(writer: Writer): String => Unit = writer.write(_: String)

    ProcessLogger(fout = writeTo(stdout), ferr = writeTo(stderr))
  }

  def unapply(job: LJob): Option[(String, Set[Output])] = job match {
    case clj: CommandLineJob => Some((clj.commandLineString, clj.outputs))
    case _                   => None
  }
}
