package loamstream.model.jobs.commandline

import java.nio.file.{Path, Files => JFiles}

import loamstream.model.execute.ExecutionEnvironment
import loamstream.model.jobs.{LJob, Output}
import loamstream.model.jobs.commandline.CommandLineJob.stdErrProcessLogger
import loamstream.util.{BashScript, Files, Loggable}

import scala.sys.process.{ProcessBuilder, ProcessLogger}

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  *
  * A job based on a command line provided as a String
  */
final case class CommandLineStringJob(
    commandLineString: String,
    workDir: Path,
    executionEnvironment: ExecutionEnvironment,
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty,
    exitValueCheck: Int => Boolean = CommandLineJob.defaultExitValueChecker,
    override val logger: ProcessLogger = stdErrProcessLogger) extends CommandLineJob with Loggable {

  override def processBuilder: ProcessBuilder =
    BashScript.fromCommandLineString(commandLineString).processBuilder(workDir)

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  def withCommandLineString(newCmd: String): CommandLineStringJob = copy(commandLineString = newCmd)
}

