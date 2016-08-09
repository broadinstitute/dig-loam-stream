package loamstream.model.jobs.commandline

import java.nio.file.Path

import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob.noOpProcessLogger

import scala.sys.process.{Process, ProcessBuilder, ProcessLogger}
import loamstream.model.jobs.Output

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  * 
  * A job based on a command line provided as a String
  */
final case class CommandLineStringJob(
    commandLineString: String, 
    workDir: Path, 
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty,
    exitValueCheck: Int => Boolean = CommandLineJob.defaultExitValueChecker,
    override val logger: ProcessLogger = noOpProcessLogger) extends CommandLineJob {
  
  override def processBuilder: ProcessBuilder = Process(commandLineString, workDir.toFile)

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
  
  override protected def doWithOutputs(newOutputs: Set[Output]): LJob = copy(outputs = newOutputs)
}
