package loamstream.compiler

import loamstream.LEnv
import loamstream.compiler.Issue.Severity
import loamstream.compiler.LoamCompiler.{CompilerReporter, DslChunk}
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.messages.{CompilerIssueMessage, StatusOutMessage}
import loamstream.loam.{GraphPrinter, LEnvBuilder, LoamGraph, LoamGraphBuilder, LoamTool}
import loamstream.tools.core.LCoreEnv
import loamstream.util.{PathEnrichments, ReflectionUtil, SourceUtils, StringUtils}

import scala.concurrent.ExecutionContext
import scala.reflect.internal.util.{AbstractFileClassLoader, BatchSourceFile, Position}
import scala.tools.nsc.Settings
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.reporters.Reporter
import scala.tools.reflect.ReflectGlobal
import scala.util.control.NonFatal

/** The compiler compiling Loam scripts into execution plans */
object LoamCompiler {

  /** A wrapper type for Loam scripts */
  trait DslChunk {
    /** The runtime environment defined by this Loam script */
    def env: LEnv

    /** The graph of stores and tools defined by this Loam script */
    def graph: LoamGraph
  }

  /** A reporter receiving messages form the underlying Scala compiler
    *
    * @param outMessageSink The recipient of the Scala compiler messages
    */
  final class CompilerReporter(outMessageSink: OutMessageSink) extends Reporter {
    var errors = Seq.empty[Issue]
    var warnings = Seq.empty[Issue]
    var infos = Seq.empty[Issue]

    /** Method called by the Scala compiler to emit errors, warnings and infos */
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

  /** The result of the compilation of a Loam script */
  object Result {
    /** Constructs a result representing successful compilation */
    def success(reporter: CompilerReporter, graph: LoamGraph, env: LEnv): Result = {
      Result(reporter.errors, reporter.warnings, reporter.infos, Some(graph), Some(env))
    }

    /** Constructs a result that the Loam script cannot be compiled */
    def failure(reporter: CompilerReporter): Result = {
      Result(reporter.errors, reporter.warnings, reporter.infos, None, None)
    }

    /** Constructs a result that an exception was thrown during compilation */
    def throwable(reporter: CompilerReporter, throwable: Throwable): Result = {
      Result(reporter.errors, reporter.warnings, reporter.infos, None, None, Some(throwable))
    }
  }

  /** The result of the compilation of a Loam script
    *
    * @param errors   Errors from the Scala compiler
    * @param warnings Warnings from the Scala compiler
    * @param infos    Infos from the Scala compiler
    * @param graphOpt Option of graph of stores and tools
    * @param envOpt   Option of runtime settings
    * @param exOpt    Option of an exception if thrown
    */
  final case class Result(errors: Seq[Issue], warnings: Seq[Issue], infos: Seq[Issue], graphOpt: Option[LoamGraph],
                          envOpt: Option[LEnv], exOpt: Option[Throwable] = None) {
    /** Returns true if no errors */
    def isValid: Boolean = errors.isEmpty

    /** Returns true if no issues */
    def isClean: Boolean = isValid && warnings.isEmpty && infos.isEmpty

    /** Returns true if graph of stores and tools and runtime settings have been found */
    def isSuccess: Boolean = envOpt.nonEmpty && graphOpt.nonEmpty

    /** One-line summary of the result */
    def summary: String = {
      val soManyErrors = StringUtils.soMany(errors.size, "error")
      val soManyWarnings = StringUtils.soMany(warnings.size, "warning")
      val soManyInfos = StringUtils.soMany(infos.size, "info")
      val soManySettings = StringUtils.soMany(envOpt.map(_.keys.size).getOrElse(0), "runtime setting")
      val soManyStores = StringUtils.soMany(graphOpt.map(_.stores.size).getOrElse(0), "store")
      val soManyTools = StringUtils.soMany(graphOpt.map(_.tools.size).getOrElse(0), "tool")
      s"There were $soManyErrors, $soManyWarnings, $soManyInfos, $soManySettings, $soManyStores and $soManyTools."
    }

    /** Detailed report listing all issues */
    def report: String = (summary +: (errors ++ warnings ++ infos).map(_.summary)).mkString(System.lineSeparator)

  }

}

/** The compiler compiling Loam scripts into execution plans */
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

  /** Wraps Loam script in template to create valid Scala file */
  def wrapCode(raw: String): String = {
    s"""
package $inputObjectPackage

import ${SourceUtils.fullTypeName[LCoreEnv.Keys.type]}._
import ${SourceUtils.fullTypeName[LoamPredef.type]}._
import ${SourceUtils.fullTypeName[LEnvBuilder]}
import ${SourceUtils.fullTypeName[LoamGraphBuilder]}
import ${SourceUtils.fullTypeName[DslChunk]}
import ${SourceUtils.fullTypeName[LEnv]}._
import ${SourceUtils.fullTypeName[LoamTool.type]}._
import ${SourceUtils.fullTypeName[PathEnrichments.type]}._
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

  /** Compiles Loam script into execution plan */
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
        val lengthOfLine = 100
        val graphPrinter = GraphPrinter.byLine(lengthOfLine)
        outMessageSink.send(StatusOutMessage(
          s"""
             |[Start Graph]
             |${graphPrinter.print(graph)}
             |[End Graph]
           """.stripMargin))
        LoamCompiler.Result.success(reporter, graph, env)
      } else {
        outMessageSink.send(StatusOutMessage(s"Compilation failed. There were $soManyIssues."))
        LoamCompiler.Result.failure(reporter)
      }
    } catch {
      case NonFatal(throwable) =>
        outMessageSink.send(
          StatusOutMessage(s"${throwable.getClass.getName} while trying to compile: ${throwable.getMessage}"))
        LoamCompiler.Result.throwable(reporter, throwable)
    }
  }

}
