package loamstream.model.jobs.commandline

import java.nio.file.Path

import scala.sys.process.ProcessBuilder
import scala.sys.process.ProcessLogger

import loamstream.model.execute.Environment
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import loamstream.model.jobs.commandline.CommandLineJob.stdErrProcessLogger
import loamstream.util.BashScript
import loamstream.util.Loggable

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  *
  * A job based on a command line provided as a String
  */
final case class CommandLineStringJob(
    commandLineString: String,
    workDir: Path,
    executionEnvironment: Environment,
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty,
    exitValueCheck: Int => Boolean = CommandLineJob.defaultExitValueChecker,
    override val logger: ProcessLogger = stdErrProcessLogger) extends CommandLineJob with Loggable {

  override def name: String = s"${getClass.getSimpleName}#${id}('${commandLineString}', ...)"
  
  override def processBuilder: ProcessBuilder = {
    BashScript.fromCommandLineString(commandLineString).processBuilder(workDir)
  }

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  def withCommandLineString(newCmd: String): CommandLineStringJob = copy(commandLineString = newCmd)
}

