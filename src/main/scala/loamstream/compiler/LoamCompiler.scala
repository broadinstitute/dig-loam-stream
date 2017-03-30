package loamstream.compiler

import loamstream.compiler.Issue.Severity
import loamstream.compiler.LoamCompiler.CompilerReporter
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.messages.{CompilerIssueMessage, ErrorOutMessage, StatusOutMessage}
import loamstream.conf.LoamConfig
import loamstream.loam.LoamScript.LoamScriptBox
import loamstream.loam.{GraphPrinter, LoamGraph, LoamGraphValidation, LoamProjectContext, LoamScript}
import loamstream.util.Validation.IssueBase
import loamstream.util.code.ReflectionUtil
import loamstream.util.{DepositBox, Loggable, NonFatalInitializer, StringUtils}

import scala.reflect.internal.util.{AbstractFileClassLoader, BatchSourceFile, Position}
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.reporters.Reporter
import scala.tools.nsc.{Settings => ScalaCompilerSettings}
import scala.tools.reflect.ReflectGlobal

/** The compiler compiling Loam scripts into execution plans */
object LoamCompiler extends Loggable {

  object Settings {
    val default: Settings = Settings(logCode = false, logCodeOnError = true)
  }

  case class Settings(logCode: Boolean, logCodeOnError: Boolean) {
    def logCodeForLevel(level: Loggable.Level.Value): Boolean =
      logCode || (logCodeOnError && (level >= Loggable.Level.warn))
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
    def success(reporter: CompilerReporter, context: LoamProjectContext): Result = {
      Result(reporter.errors, reporter.warnings, reporter.infos, Some(context))
    }

    /** Constructs a result that the Loam script cannot be compiled */
    def failure(reporter: CompilerReporter): Result = {
      Result(reporter.errors, reporter.warnings, reporter.infos, None)
    }

    /** Constructs a result that an exception was thrown during compilation */
    def throwable(reporter: CompilerReporter, throwable: Throwable): Result = {
      Result(reporter.errors, reporter.warnings, reporter.infos, None, Some(throwable))
    }
  }

  /** The result of the compilation of a Loam script
    *
    * @param errors     Errors from the Scala compiler
    * @param warnings   Warnings from the Scala compiler
    * @param infos      Infos from the Scala compiler
    * @param contextOpt Option of a context with graph of stores and tools
    * @param exOpt      Option of an exception if thrown
    */
  final case class Result(errors: Seq[Issue], warnings: Seq[Issue], infos: Seq[Issue],
                          contextOpt: Option[LoamProjectContext], exOpt: Option[Throwable] = None) {
    /** Returns true if no errors */
    def isValid: Boolean = errors.isEmpty

    /** Returns true if no issues */
    def isClean: Boolean = isValid && warnings.isEmpty && infos.isEmpty

    /** Returns true if graph of stores and tools has been found */
    def isSuccess: Boolean = contextOpt.nonEmpty

    /** One-line summary of the result */
    def summary: String = {
      val soManyErrors = StringUtils.soMany(errors.size, "error")
      val soManyWarnings = StringUtils.soMany(warnings.size, "warning")
      val soManyInfos = StringUtils.soMany(infos.size, "info")
      val soManyStores = StringUtils.soMany(contextOpt.map(_.graph.stores.size).getOrElse(0), "store")
      val soManyTools = StringUtils.soMany(contextOpt.map(_.graph.tools.size).getOrElse(0), "tool")
      s"There were $soManyErrors, $soManyWarnings, $soManyInfos, $soManyStores and $soManyTools."
    }

    /** Detailed report listing all issues */
    def report: String = (summary +: (errors ++ warnings ++ infos).map(_.summary)).mkString(System.lineSeparator)
  }

  def withLogging(settings: LoamCompiler.Settings = LoamCompiler.Settings.default): LoamCompiler =
    LoamCompiler(settings, OutMessageSink.LoggableOutMessageSink(this))

  def apply(settings: Settings): LoamCompiler = new LoamCompiler(settings)

  def apply(outMessageSink: OutMessageSink): LoamCompiler =
    new LoamCompiler(Settings.default, outMessageSink)

  def apply(settings: Settings, outMessageSink: OutMessageSink): LoamCompiler =
    new LoamCompiler(settings, outMessageSink)

}

/** The compiler compiling Loam scripts into execution plans */
final class LoamCompiler(settings: LoamCompiler.Settings = LoamCompiler.Settings.default,
                         outMessageSink: OutMessageSink = OutMessageSink.NoOp) extends Loggable {

  val targetDirectoryName = "target"
  val targetDirectoryParentOption = None
  val targetDirectory = new VirtualDirectory(targetDirectoryName, targetDirectoryParentOption)
  val scalaCompilerSettings = new ScalaCompilerSettings
  scalaCompilerSettings.outputDirs.setSingleOutput(targetDirectory)
  val reporter = new CompilerReporter(outMessageSink)
  val compileLock = new AnyRef
  val compiler = compileLock.synchronized {
    val classLoader = new LoamClassLoader(getClass.getClassLoader)
    new ReflectGlobal(scalaCompilerSettings, reporter, classLoader)
  }

  def soManyIssues: String = {
    val soManyErrors = StringUtils.soMany(reporter.errorCount, "error")
    val soManyWarnings = StringUtils.soMany(reporter.warningCount, "warning")
    s"$soManyErrors and $soManyWarnings"
  }

  def logScripts(logLevel: Loggable.Level.Value, project: LoamProject, graphBoxReceipt: DepositBox.Receipt): Unit = {
    if (settings.logCodeForLevel(logLevel)) {
      for (script <- project.scripts) {
        log(logLevel, script.scalaId.toString)
        log(logLevel, script.asScalaCode(graphBoxReceipt))
      }
    }
  }

  def validateGraph(graph: LoamGraph): Seq[IssueBase[LoamGraph]] = {
    val graphIssues = LoamGraphValidation.allRules(graph)
    if (graphIssues.isEmpty) {
      outMessageSink.send(StatusOutMessage("Execution graph successfully validated."))
    } else {
      outMessageSink.send(
        ErrorOutMessage(s"Execution graph validation found ${StringUtils.soMany(graphIssues.size, "issue")}")
      )
      for(graphIssue <- graphIssues) {
        outMessageSink.send(ErrorOutMessage(graphIssue.message))
      }
    }
    graphIssues
  }

  def reportCompilation(project: LoamProject, graph: LoamGraph, graphBoxReceipt: DepositBox.Receipt): Unit = {
    val soManyStores = StringUtils.soMany(graph.stores.size, "store")
    val soManyTools = StringUtils.soMany(graph.tools.size, "tool")
    outMessageSink.send(StatusOutMessage(s"Found $soManyStores and $soManyTools."))
    val lengthOfLine = 100
    val graphPrinter = GraphPrinter.byLine(lengthOfLine)
    val logLevel = if (reporter.hasErrors) {
      Loggable.Level.error
    } else if (reporter.hasWarnings) {
      Loggable.Level.warn
    } else {
      Loggable.Level.info
    }
    logScripts(logLevel, project, graphBoxReceipt)
    outMessageSink.send(StatusOutMessage(
      s"""
         |[Start Graph]
         |${
        graphPrinter.print(graph)
      }
         |[End Graph]
           """.stripMargin))
  }

  /** Compiles Loam script into execution plan */
  def compile(config: LoamConfig, script: LoamScript): LoamCompiler.Result = compile(LoamProject(config, script))

  /** Compiles Loam script into execution plan */
  def compile(project: LoamProject): LoamCompiler.Result = compileLock.synchronized {
    val projectContextReceipt = LoamProjectContext.depositBox.deposit(LoamProjectContext.empty(project.config))
    try {
      val sourceFiles = project.scripts.map { script =>
        new BatchSourceFile(script.scalaFileName, script.asScalaCode(projectContextReceipt))
      }
      reporter.reset()
      targetDirectory.clear()
      val run = new compiler.Run
      run.compileSources(sourceFiles.toList)
      if (targetDirectory.nonEmpty) {
        outMessageSink.send(StatusOutMessage(s"Completed compilation and there were $soManyIssues."))
        val classLoader = new AbstractFileClassLoader(targetDirectory, getClass.getClassLoader)
        val scriptBoxes = project.scripts.map { script =>
          ReflectionUtil.getObject[LoamScriptBox](classLoader, script.scalaId)
        }
        val scriptBox = scriptBoxes.head
        val graph = scriptBox.graph
        validateGraph(graph)
        reportCompilation(project, graph, projectContextReceipt)
        LoamCompiler.Result.success(reporter, scriptBox.projectContext)
      } else {
        logScripts(Loggable.Level.error, project, projectContextReceipt)
        outMessageSink.send(StatusOutMessage(s"Compilation failed. There were $soManyIssues."))
        LoamCompiler.Result.failure(reporter)
      }
    } catch {
      case NonFatalInitializer(throwable) =>
        logScripts(Loggable.Level.error, project, projectContextReceipt)
        outMessageSink.send(
          StatusOutMessage(s"${
            throwable.getClass.getName
          } while trying to compile: ${
            throwable.getMessage
          }"))
        LoamCompiler.Result.throwable(reporter, throwable)
    } finally {
      LoamProjectContext.depositBox.remove(projectContextReceipt)
    }
  }
}
