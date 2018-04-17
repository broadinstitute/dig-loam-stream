package loamstream.model.jobs.commandline

import java.nio.file.Path

import loamstream.model.execute.Environment
import loamstream.model.jobs.{JobNode, LJob, Output}
import loamstream.util.Loggable

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
    override val inputs: Set[JobNode] = Set.empty,
    outputs: Set[Output] = Set.empty,
    exitValueCheck: Int => Boolean = CommandLineJob.defaultExitValueChecker,
    private val nameOpt: Option[String] = None,
    dockerLocationOpt: Option[String] = None) extends HasCommandLine with Loggable {

  override def equals(other: Any): Boolean = other match {
    case that: CommandLineJob => this.id == that.id
    case _ => false
  }
  
  override def hashCode: Int = id.hashCode
  
  override def name: String = nameOpt.getOrElse(id.toString)

  def withCommandLineString(newCmd: String): CommandLineJob = copy(commandLineString = newCmd)

  override def workDirOpt: Option[Path] = Some(workDir)

  def withDockerLocation(dockerLocation: String): CommandLineJob = copy(dockerLocationOpt = Some(dockerLocation))

  def exitValueIsOk(exitValue: Int): Boolean = exitValueCheck(exitValue)

  override def toString: String = s"${getClass.getSimpleName}#${id}('${commandLineString}', ...)"
}

object CommandLineJob extends Loggable {

  private val mustBeZero: Int => Boolean = _ == 0

  val defaultExitValueChecker: Int => Boolean = mustBeZero

  def unapply(job: LJob): Option[(String, Set[Output])] = job match {
    case clj: CommandLineJob => Some((clj.commandLineString, clj.outputs))
    case _                   => None
  }
}
