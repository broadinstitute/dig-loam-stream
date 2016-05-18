package loamstream.web.parser

import java.io.File

import loamstream.LEnv
import loamstream.tools.core.LCoreEnv
import loamstream.util.{LEnvBuilder, ReflectionUtil, SourceUtils, StringUtils}
import loamstream.web.controllers.socket.CompilerOutMessage.Severity
import loamstream.web.controllers.socket.SocketMessageHandler.OutMessageSink
import loamstream.web.controllers.socket.{CompilerOutMessage, StatusOutMessage}
import loamstream.web.parser.LoamCompiler.{CompilerReporter, DslChunk}

import scala.concurrent.ExecutionContext
import scala.reflect.internal.util.{AbstractFileClassLoader, BatchSourceFile, Position}
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.reporters.Reporter
import scala.tools.nsc.{Global, Settings}
import scala.util.{Failure, Success, Try}

/**
  * LoamStream
  * Created by oliverr on 5/10/2016.
  */
object LoamCompiler {

  trait DslChunk {
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
  val classLoader = new AbstractFileClassLoader(targetDirectory, getClass.getClassLoader)

  val inputObjectPackage = "loamstream.dynamic.input"
  val inputObjectName = "Some" + SourceUtils.shortTypeName[DslChunk]
  val inputObjectFullName = s"$inputObjectPackage.$inputObjectName"

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
    s"""
package $inputObjectPackage

import ${SourceUtils.fullTypeName[LCoreEnv.Keys.type]}._
import ${SourceUtils.fullTypeName[LCoreEnv.Helpers.type]}._
import ${SourceUtils.fullTypeName[LCoreEnv.Implicits.type]}._
import ${SourceUtils.fullTypeName[LEnvBuilder]}
import ${SourceUtils.fullTypeName[DslChunk]}
import ${SourceUtils.fullTypeName[LEnv]}._
import java.nio.file._

object $inputObjectName extends ${SourceUtils.shortTypeName[DslChunk]} {
implicit val envBuilder = new LEnvBuilder

${raw.trim}

def env = envBuilder.toEnv
}
"""

  def compile(rawCode: String): Unit = {
    val wrappedCode = wrapCode(rawCode)
    val sourceFile = new BatchSourceFile(sourceFileName, wrappedCode)
    reporter.reset()
    val run = new compiler.Run
    run.compileSources(List(sourceFile))
    outMessageSink.send(StatusOutMessage(s"Completed compilation and there were $soManyIssues."))
    val dslChunk = ReflectionUtil.getObject[DslChunk](classLoader, inputObjectFullName)
    val env = dslChunk.env
    outMessageSink.send(StatusOutMessage(s"Found ${StringUtils.soMany(env.size, "runtime setting")}."))
  }

}
