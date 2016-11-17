package loamstream.model.jobs.commandline

import java.nio.file.Path
import java.util.regex.Matcher

import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob.stdErrProcessLogger

import scala.sys.process.{Process, ProcessBuilder, ProcessLogger}
import loamstream.model.jobs.Output
import loamstream.util.StringUtils
import loamstream.util.Loggable
import loamstream.util.PlatformUtil

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
    override val logger: ProcessLogger = stdErrProcessLogger) extends CommandLineJob with Loggable {

  override def processBuilder: ProcessBuilder = {
    val tokens = CommandLineStringJob.tokensToRun(commandLineString)
    
    debug(s"Escaped command tokens: $tokens")
    
    Process(tokens, workDir.toFile)
  }

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}

object CommandLineStringJob {
  
  private[commandline] def escapeCommandString(s: String): String = {
    if(!PlatformUtil.isWindows) { s }
    else {
      import Matcher.quoteReplacement
      
      quoteReplacement(quoteReplacement(s))
    }
  }
  
  private[commandline] def tokensToRun(commandString: String): Seq[String] = Seq("sh", "-c", escapeCommandString(commandString))
}
