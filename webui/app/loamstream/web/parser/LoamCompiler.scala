package loamstream.web.parser

import java.io.File

import loamstream.util.StringUtils
import loamstream.web.controllers.socket.CompilerOutMessage.Severity
import loamstream.web.controllers.socket.SocketMessageHandler.OutMessageSink
import loamstream.web.controllers.socket.{CompilerOutMessage, StatusOutMessage}
import loamstream.web.parser.LoamCompiler.CompilerReporter

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.reflect.internal.util.{BatchSourceFile, Position}
import scala.tools.nsc.Settings
import scala.tools.nsc.interactive.{Global, Response}
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.reporters.Reporter
import scala.util.{Failure, Success, Try}
import scala.reflect.internal.Trees

/**
  * LoamStream
  * Created by oliverr on 5/10/2016.
  */
object LoamCompiler {

  class CompilerReporter(outMessageSink: OutMessageSink) extends Reporter {
    override protected def info0(pos: Position, msg: String, severity: Severity, force: Boolean): Unit = {
      severity.id match {
        case 2 => this.ERROR.count += 1
        case 1 => this.WARNING.count += 1
        case _ => ()
      }
      outMessageSink.send(CompilerOutMessage(pos, msg, Severity(severity.id), force))
    }
  }

}

class LoamCompiler(outMessageSink: OutMessageSink)(implicit executionContext: ExecutionContext) {

  val targetDirectoryName = "target"
  val targetDirectoryParentOption = None
  val targetDirectory = new VirtualDirectory(targetDirectoryName, targetDirectoryParentOption)
  val settings = new Settings()
  settings.outputDirs.setSingleOutput(targetDirectory)
  val sbtClasspath = System.getProperty("sbt-classpath")
  settings.classpath.value = s".${File.pathSeparator}$sbtClasspath"
  val reporter = new CompilerReporter(outMessageSink)
  val compiler = new Global(settings, reporter)
  val sourceFileName = "Config.scala"

  def soManyIssues: String = {
    val soManyErrors = StringUtils.soMany(reporter.ERROR.count, "error")
    val soManyWarnings = StringUtils.soMany(reporter.WARNING.count, "warning")
    s"there were $soManyErrors and $soManyWarnings"
  }

  def sendOutResponse(responseValue: Try[Option[Either[compiler.Tree, Throwable]]]): Unit = {
    val outMessageTextEnd = responseValue match {
      case Success(Some(Left(_))) => s"There were $soManyIssues."
      case Success(Some(Right(ex: InterruptedException))) => "Compiler was interrupted."
      case Success(Some(Right(ex))) => "Packaged Exception: " + ex.getMessage
      case Success(None) => "Compiler tried to reload and timed out."
      case Failure(ex) => "Exception was thrown: " + ex.getMessage
    }
    outMessageSink.send(StatusOutMessage(outMessageTextEnd))
  }

  def compile(codeString: String): Unit = {
    val sourceFile = new BatchSourceFile(sourceFileName, codeString)
    val response = new Response[compiler.Tree]
    compiler.askLoadedTyped(sourceFile, response)
    val timeOutMs = 30000
    val responseFut = Future(blocking(response.get(timeOutMs)))
    responseFut.onComplete(sendOutResponse)
  }

}
