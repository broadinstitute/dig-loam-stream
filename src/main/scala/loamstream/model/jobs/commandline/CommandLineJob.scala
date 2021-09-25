package loamstream.model.jobs.commandline

import java.nio.file.Path
import java.nio.file.Paths

import loamstream.model.execute.Settings
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.LJob
import loamstream.util.Loggable

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
    protected override val successorsFn: () => Set[JobNode] = () => Set.empty,
    inputs: Set[DataHandle] = Set.empty,
    outputs: Set[DataHandle] = Set.empty,
    exitValueCheck: Int => Boolean = CommandLineJob.defaultExitValueChecker,
    private val nameOpt: Option[String] = None) extends HasCommandLine with JobNode.LazySucessors with Loggable {

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
