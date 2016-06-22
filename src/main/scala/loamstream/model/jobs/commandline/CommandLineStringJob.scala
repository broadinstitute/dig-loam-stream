package loamstream.model.jobs.commandline

import java.nio.file.Path

import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob.noOpProcessLogger

import scala.sys.process.{Process, ProcessBuilder, ProcessLogger}

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
case class CommandLineStringJob(commandLineString: String, workDir: Path, inputs: Set[LJob] = Set.empty,
                                exitValueCheck: Int => Boolean = CommandLineJob.acceptAll,
                                override val logger: ProcessLogger = noOpProcessLogger)
  extends CommandLineJob {
  override def processBuilder: ProcessBuilder = Process(commandLineString, workDir.toFile)

  override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}
