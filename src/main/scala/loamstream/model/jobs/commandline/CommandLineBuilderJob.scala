package loamstream.model.jobs.commandline

import java.nio.file.Path

import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob.noOpProcessLogger
import loamstream.tools.LineCommand.CommandLine

import scala.sys.process.{Process, ProcessBuilder, ProcessLogger}

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */

/** A job based on a programmatically built command line. */
final case class CommandLineBuilderJob(commandLine: CommandLine,
                                       workDir: Path,
                                       inputs: Set[LJob] = Set.empty,
                                       exitValueCheck: Int => Boolean = CommandLineJob.defaultExitValueChecker,
                                       override val logger: ProcessLogger = noOpProcessLogger)
  extends CommandLineJob {

  override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

  override def processBuilder: ProcessBuilder = Process(commandLine.tokens, workDir.toFile)

  override def commandLineString: String = commandLine.toString
}
