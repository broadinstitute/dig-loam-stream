package loamstream.web.parser

import java.io.File

import loamstream.LEnv
import loamstream.tools.core.LCoreEnv
import loamstream.util.{LEnvBuilder, SourceUtils, StringUtils}
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
import loamstream.web.parser.LoamCompiler.DSLChunk

/**
  * LoamStream
  * Created by oliverr on 5/10/2016.
  */
object LoamCompiler {

  trait DSLChunk {
    def env: LEnv
  }

  class CompilerReporter(outMessageSink: OutMessageSink) extends Reporter {
    override protected def info0(pos: Position, msg: String, severity: Severity, force: Boolean): Unit = {
      severity.id match {
        case 2 => this.ERROR.count += 1
        case 1 => this.WARNING.count += 1
        case _ => ()
      }
      val message = s"[$severity] $msg"
      outMessageSink.send(CompilerOutMessage(pos, message, Severity(severity.id), force))
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
    val soManyErrors = StringUtils.soMany(reporter.errorCount, "error")
    val soManyWarnings = StringUtils.soMany(reporter.warningCount, "warning")
    s"$soManyErrors and $soManyWarnings"
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

  def wrapCode(raw: String): String =
    s"""package loamstream.dynamic.input
        |
        |import ${SourceUtils.likelyTypeName(LCoreEnv.Keys)}._
        |import ${SourceUtils.likelyTypeName(LCoreEnv.Helpers)}._
        |import ${SourceUtils.likelyTypeName(LCoreEnv.Implicits)}._
        |import ${SourceUtils.likelyTypeName[LEnvBuilder]}
        |import ${SourceUtils.likelyTypeName[DSLChunk]}
        |import ${SourceUtils.likelyTypeName(LEnv)}._
        |import java.nio.file._
        |
        |object SomeDSLChunk extends DSLChunk {
        |implicit val envBuilder = new LEnvBuilder
        |
        |${raw.trim}
        |
        |def env = envBuilder.toEnv
        |}
     """.stripMargin

  def compile(rawCode: String): Unit = {
    val wrappedCode = wrapCode(rawCode)
    val sourceFile = new BatchSourceFile(sourceFileName, wrappedCode)
    val response = new Response[compiler.Tree]
    reporter.reset()
    compiler.askLoadedTyped(sourceFile, response)
    val timeOutMs = 30000
    val responseFut = Future(blocking(response.get(timeOutMs)))
    responseFut.onComplete(sendOutResponse)
  }

}
