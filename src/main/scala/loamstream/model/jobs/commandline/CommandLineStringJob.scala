package loamstream.model.jobs.commandline

import java.nio.file.Path
import java.util.regex.Matcher

import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob.stdErrProcessLogger

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
    override val logger: ProcessLogger = stdErrProcessLogger) extends CommandLineJob {

  override def processBuilder: ProcessBuilder = {
    val commandLineEncoded = Matcher.quoteReplacement(commandLineString)
    val commandLineEncodedEncoded = Matcher.quoteReplacement(commandLineEncoded)
    Process(Seq("bash", "-c", commandLineEncodedEncoded), workDir.toFile)
  }

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}
