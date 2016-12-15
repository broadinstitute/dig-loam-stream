package loamstream.model.jobs.commandline

import java.nio.file.{Path, Files => JFiles}
import java.util.regex.Matcher

import scala.sys.process.Process
import scala.sys.process.ProcessBuilder
import scala.sys.process.ProcessLogger
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import loamstream.model.jobs.commandline.CommandLineJob.stdErrProcessLogger
import loamstream.util.{BashScript, Files, Loggable, PlatformUtil}

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

  override def processBuilder: ProcessBuilder =
    BashScript.fromCommandLineString(commandLineString).processBuilder(workDir)

  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}

object CommandLineStringJob {

  private[commandline] def tokensToRun(commandString: String): Seq[String] = {
    val scriptFile = JFiles.createTempFile("cmd", "sh")
    Files.writeTo(scriptFile)(commandString.replace("\\", "\\\\\\\\") + "\nexit\n")
    Seq("sh", scriptFile.toString.replace("\\", "\\\\"))
  }
}
