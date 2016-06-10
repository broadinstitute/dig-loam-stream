package loamstream.compiler

import loamstream.LEnv
import loamstream.compiler.ClientMessageHandler.OutMessageSink
import loamstream.compiler.Issue.Severity
import loamstream.compiler.LoamCompiler.{CompilerReporter, DslChunk}
import loamstream.loam.{GraphPrinter, LEnvBuilder, LoamGraph, LoamGraphBuilder, ToolBuilder}
import loamstream.tools.core.LCoreEnv
import loamstream.util.{ReflectionUtil, SourceUtils, StringUtils}

import scala.concurrent.ExecutionContext
import scala.reflect.internal.util.{AbstractFileClassLoader, BatchSourceFile, Position}
import scala.tools.nsc.Settings
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.reporters.Reporter
import scala.tools.reflect.ReflectGlobal
import scala.util.{Failure, Success, Try}

/**
  * LoamStream
  * Created by oliverr on 5/10/2016.
  */
object LoamCompiler {

  trait DslChunk {
    def env: LEnv

    def graph: LoamGraph
  }

  final class CompilerReporter(outMessageSink: OutMessageSink) extends Reporter {
    var errors = Seq.empty[Issue]
    var warnings = Seq.empty[Issue]
    var infos = Seq.empty[Issue]

    override protected def info0(pos: Position, msg: String, severity: Severity, force: Boolean): Unit = {
      val issue = Issue(pos, msg, Severity(severity.id))
      severity.id match {
        case 2 => errors :+= issue
        case 1 => warnings :+= issue
        case _ => infos :+= issue
      }
      if (issue.severity.isProblem) {
        outMessageSink.send(CompilerIssueMessage(issue))
      }
    }
  }

  object Result {
    def success(reporter: CompilerReporter, env: LEnv): Result = {
      Result(reporter.errors, reporter.warnings, reporter.infos, Some(env))
    }

    def failure(reporter: CompilerReporter): Result = {
      Result(reporter.errors, reporter.warnings, reporter.infos, None)
    }

    def exception(reporter: CompilerReporter, exception: Exception): Result = {
      Result(reporter.errors, reporter.warnings, reporter.infos, None, Some(exception))
    }
  }

  final case class Result(errors: Seq[Issue], warnings: Seq[Issue], infos: Seq[Issue], envOpt: Option[LEnv],
                          exOpt: Option[Exception] = None) {
    def isSuccess: Boolean = envOpt.nonEmpty
  }

}

class LoamCompiler(outMessageSink: OutMessageSink)(implicit executionContext: ExecutionContext) {

  val targetDirectoryName = "target"
  val targetDirectoryParentOption = None
  val targetDirectory = new VirtualDirectory(targetDirectoryName, targetDirectoryParentOption)
  val settings = new Settings()
  settings.outputDirs.setSingleOutput(targetDirectory)
  val reporter = new CompilerReporter(outMessageSink)
  val compiler = new ReflectGlobal(settings, reporter, getClass.getClassLoader)
  val sourceFileName = "Config.scala"

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

  def wrapCode(raw: String): String = {
    s"""
package $inputObjectPackage

import ${SourceUtils.fullTypeName[LCoreEnv.Keys.type]}._
import ${SourceUtils.fullTypeName[LoamPredef.type]}._
import ${SourceUtils.fullTypeName[LEnvBuilder]}
import ${SourceUtils.fullTypeName[LoamGraphBuilder]}
import ${SourceUtils.fullTypeName[DslChunk]}
import ${SourceUtils.fullTypeName[LEnv]}._
import ${SourceUtils.fullTypeName[ToolBuilder.type]}._
import loamstream.dsl._
import java.nio.file._

object $inputObjectName extends ${SourceUtils.shortTypeName[DslChunk]} {
implicit val envBuilder = new LEnvBuilder
implicit val graphBuilder = new LoamGraphBuilder

${raw.trim}

def env = envBuilder.toEnv
def graph = graphBuilder.graph
}
"""
  }

  def compile(rawCode: String): LoamCompiler.Result = {
    try {
      val wrappedCode = wrapCode(rawCode)
      val sourceFile = new BatchSourceFile(sourceFileName, wrappedCode)
      reporter.reset()
      targetDirectory.clear()
      val run = new compiler.Run
      run.compileSources(List(sourceFile))
      if (targetDirectory.nonEmpty) {
        outMessageSink.send(StatusOutMessage(s"Completed compilation and there were $soManyIssues."))
        val classLoader = new AbstractFileClassLoader(targetDirectory, getClass.getClassLoader)
        val dslChunk = ReflectionUtil.getObject[DslChunk](classLoader, inputObjectFullName)
        val env = dslChunk.env
        val graph = dslChunk.graph
        val stores = graph.stores
        val tools = graph.tools
        val soManySettings = StringUtils.soMany(env.size, "runtime setting")
        val soManyStores = StringUtils.soMany(stores.size, "store")
        val soManyTools = StringUtils.soMany(tools.size, "tool")
        outMessageSink.send(StatusOutMessage(s"Found $soManySettings, $soManyStores and $soManyTools."))
        val idLength = 4
        val graphPrinter = new GraphPrinter(idLength)
        outMessageSink.send(StatusOutMessage(
          s"""
             |[Start Graph]
             |${graphPrinter.print(graph)}
             |[End Graph]
           """.stripMargin))
        LoamCompiler.Result.success(reporter, env)
      } else {
        outMessageSink.send(StatusOutMessage(s"Compilation failed. There were $soManyIssues."))
        LoamCompiler.Result.failure(reporter)
      }
    } catch {
      case exception: Exception =>
        outMessageSink.send(
          StatusOutMessage(s"${exception.getClass.getName} while trying to compile: ${exception.getMessage}"))
        LoamCompiler.Result.exception(reporter, exception)
    }
  }

}
