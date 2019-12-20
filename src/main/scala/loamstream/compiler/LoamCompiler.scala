package loamstream.compiler

import scala.reflect.internal.util.AbstractFileClassLoader
import scala.reflect.internal.util.BatchSourceFile
import scala.reflect.internal.util.Position
import scala.tools.nsc.{ Settings => ScalaCompilerSettings }
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.reporters.Reporter
import scala.tools.reflect.ReflectGlobal

import loamstream.compiler.Issue.Severity
import loamstream.compiler.LoamCompiler.CompilerReporter
import loamstream.conf.LoamConfig
import loamstream.loam.GraphPrinter
import loamstream.loam.LoamGraph
import loamstream.loam.LoamGraphValidation
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScript
import loamstream.loam.LoamScript.LoamScriptBox
import loamstream.util.DepositBox
import loamstream.util.Loggable
import loamstream.util.StringUtils
import loamstream.util.Validation.IssueBase
import loamstream.util.code.ReflectionUtil
import java.io.FileOutputStream
import java.io.PrintStream
import loamstream.util.TimeUtils
import loamstream.conf.CompilationConfig
import loamstream.util.HeterogeneousMap

/** The compiler compiling Loam scripts into execution plans */
object LoamCompiler extends Loggable {

  object Settings {
    val default: Settings = Settings(logCode = false, logCodeOnError = true)
  }

  final case class Settings(logCode: Boolean, logCodeOnError: Boolean) {
    def logCodeForLevel(level: Loggable.Level): Boolean = {
      logCode || (logCodeOnError && (level >= Loggable.Level.Warn))
    }
  }

  /**
   * A reporter receiving messages form the underlying Scala compiler
   *
   * @param outMessageSink The recipient of the Scala compiler messages
   */
  private final class CompilerReporter extends Reporter {
    @volatile private[LoamCompiler] var errors: Seq[Issue] = Seq.empty
    @volatile private[LoamCompiler] var warnings: Seq[Issue] = Seq.empty
    @volatile private[LoamCompiler] var infos: Seq[Issue] = Seq.empty

    /** Method called by the Scala compiler to emit errors, warnings and infos */
    override protected def info0(pos: Position, msg: String, severity: Severity, force: Boolean): Unit = {
      val issue = Issue(pos, msg, Severity(severity.id))

      severity.id match {
        case 2 => errors :+= issue
        case 1 => warnings :+= issue
        case _ => infos :+= issue
      }

      if (issue.severity.isProblem) {
        debug(issue.summary)
      }
    }
  }

  /** The result of the compilation of a Loam script */
  object Result {
    /** Constructs a result representing successful compilation */
    def success(warnings: Seq[Issue], infos: Seq[Issue], context: LoamProjectContext): Success = {
      Success(warnings, infos, context.graph)
    }

    /** Constructs a result that the Loam script cannot be compiled */
    def failure(reporter: CompilerReporter): Failure = {
      Failure(reporter.errors, reporter.warnings, reporter.infos)
    }

    /** Constructs a result that an exception was thrown during compilation */
    def throwable(reporter: CompilerReporter, throwable: Throwable): FailureDueToException = {
      FailureDueToException(reporter.errors, reporter.warnings, reporter.infos, throwable)
    }

    import StringUtils.soMany
    
    final case class Success(
        warnings: Seq[Issue],
        infos: Seq[Issue],
        graph: LoamGraph) extends Result {

      override def errors: Seq[Issue] = Nil
      
      /** Returns true if graph of stores and tools has been found */
      override def isSuccess: Boolean = true

      /** One-line summary of the result */
      override def summary: String = {
        val soManyErrors = soMany(errors.size, "error")
        val soManyWarnings = soMany(warnings.size, "warning")
        val soManyInfos = soMany(infos.size, "info")
        val soManyStores = soMany(graph.stores.size, "store")
        val soManyTools = soMany(graph.tools.size, "tool")
        s"There were $soManyErrors, $soManyWarnings, $soManyInfos, $soManyStores and $soManyTools."
      }
    }
    
    final case class Failure(
        errors: Seq[Issue],
        warnings: Seq[Issue],
        infos: Seq[Issue]) extends Result {

      /** Returns true if graph of stores and tools has been found */
      override def isSuccess: Boolean = false

      /** One-line summary of the result */
      override def summary: String = {
        val soManyErrors = soMany(errors.size, "error")
        val soManyWarnings = soMany(warnings.size, "warning")
        val soManyInfos = soMany(infos.size, "info")
        
        s"There were $soManyErrors, $soManyWarnings, $soManyInfos."
      }
    }
    
    final case class FailureDueToException(
        errors: Seq[Issue],
        warnings: Seq[Issue],
        infos: Seq[Issue],
        throwable: Throwable) extends Result {

      /** Returns true if graph of stores and tools has been found */
      override def isSuccess: Boolean = false

      /** One-line summary of the result */
      override def summary: String = {
        val soManyErrors = soMany(errors.size, "error")
        val soManyWarnings = soMany(warnings.size, "warning")
        val soManyInfos = soMany(infos.size, "info")
        
        val throwableClass = throwable.getClass.getName
        
        s"There were $soManyErrors, $soManyWarnings, $soManyInfos. ${throwableClass} caught: ${throwable.getMessage}"
      }
    }
  }

  /**
   * The result of the compilation of a Loam script
   *
   * @param errors     Errors from the Scala compiler
   * @param warnings   Warnings from the Scala compiler
   * @param infos      Infos from the Scala compiler
   * @param contextOpt Option of a context with graph of stores and tools
   * @param exOpt      Option of an exception if thrown
   */
  trait Result {
    def errors: Seq[Issue]
    def warnings: Seq[Issue]
    def infos: Seq[Issue]

    final def humanReadableErrors: Seq[CompilationError] = errors.map(CompilationError.from)

    /** Returns true if no errors */
    final def isValid: Boolean = errors.isEmpty

    /** Returns true if no issues */
    final def isClean: Boolean = isValid && warnings.isEmpty && infos.isEmpty

    /** Returns true if graph of stores and tools has been found */
    def isSuccess: Boolean

    def summary: String

    /** Detailed report listing all issues */
    //NB: Errors are handled specially elsewhere
    final def report: String = (summary +: (warnings ++ infos).map(_.summary)).mkString(System.lineSeparator)
  }

  def default: LoamCompiler = apply(CompilationConfig.default, Settings.default)

  def apply(config: CompilationConfig, settings: Settings): LoamCompiler = new LoamCompiler(config, settings)

  private def makeScalaCompilerSettings(targetDir: VirtualDirectory): ScalaCompilerSettings = {
    val scalaCompilerSettings = new ScalaCompilerSettings

    scalaCompilerSettings.outputDirs.setSingleOutput(targetDir)

    scalaCompilerSettings
  }

  private def toBatchSourceFile(projectContextReceipt: DepositBox.Receipt)(script: LoamScript) = {
    new BatchSourceFile(script.scalaFileName, script.asScalaCode(projectContextReceipt))
  }

  private def evaluateLoamScript(classLoader: ClassLoader)(script: LoamScript): LoamScriptBox = {
    ReflectionUtil.getObject[LoamScriptBox](classLoader, script.scalaId)
  }
}

/** The compiler compiling Loam scripts into execution plans */
final class LoamCompiler(
    compilationConfig: CompilationConfig,
    settings: LoamCompiler.Settings = LoamCompiler.Settings.default) extends Loggable {

  private val targetDirectoryName = "target"
  private val targetDirectoryParentOption = None
  private val targetDirectory = new VirtualDirectory(targetDirectoryName, targetDirectoryParentOption)
  private val scalaCompilerSettings = LoamCompiler.makeScalaCompilerSettings(targetDirectory)
  private val reporter = new CompilerReporter

  private[this] val compileLock = new AnyRef

  //NB: Package-private for tests
  private[compiler] val compiler = compileLock.synchronized {
    val classLoader = new LoamClassLoader(getClass.getClassLoader)
    new ReflectGlobal(scalaCompilerSettings, reporter, classLoader)
  }

  private def soManyIssues: String = {
    val soManyErrors = StringUtils.soMany(reporter.errorCount, "error")
    val soManyWarnings = StringUtils.soMany(reporter.warningCount, "warning")
    s"$soManyErrors and $soManyWarnings"
  }

  private def logScripts(
    logLevel: Loggable.Level,
    project: LoamProject,
    graphBoxReceipt: DepositBox.Receipt): Unit = {

    if (settings.logCodeForLevel(logLevel)) {
      for (script <- project.scripts) {
        log(logLevel, script.scalaId.toString)
        log(logLevel, script.asScalaCode(graphBoxReceipt))
      }
    }
  }

  private def validateGraph(graph: LoamGraph): Seq[IssueBase[LoamGraph]] = {
    val graphIssues = LoamGraphValidation.allRules(graph)

    if (graphIssues.isEmpty) {
      debug("Execution graph successfully validated.")
    } else {
      warn(s"Execution graph validation found ${StringUtils.soMany(graphIssues.size, "issue")}")

      for (graphIssue <- graphIssues) {
        warn(graphIssue.message)
      }
    }

    graphIssues
  }

  private def logCompilationErrors(result: LoamCompiler.Result): LoamCompiler.Result = {
    val humanReadableErrors = result.humanReadableErrors

    error(s"There were ${humanReadableErrors.size} compilation errors")

    result.humanReadableErrors.map { err =>
      val newLine = System.lineSeparator

      val errString = s"$newLine-------------------------------$newLine${err.toHumanReadableString}"

      error(errString)
    }

    result
  }

  private def reportCompilation(project: LoamProject, graph: LoamGraph, graphBoxReceipt: DepositBox.Receipt): Unit = {
    val lengthOfLine = 100
    val graphPrinter = GraphPrinter.byLine(lengthOfLine)

    logScripts(Loggable.Level.Trace, project, graphBoxReceipt)

    trace(s"""|[Start Graph]
              |${graphPrinter.print(graph)}
              |[End Graph]""".stripMargin)
  }

  private def withRun[A](f: compiler.Run => A): A = {
    reporter.reset()
    targetDirectory.clear()

    f(new compiler.Run)
  }

  private def depositProjectContextAndThen[A](
      project: LoamProject, 
      propertiesForLoamCode: Iterable[HeterogeneousMap.Entry[_, _]])(f: DepositBox.Receipt => A): A = {
    
    val projectContext = LoamProjectContext.empty(project.config)
    
    projectContext.addToPropertiesMap(propertiesForLoamCode.toSeq: _*)
    
    val projectContextReceipt = LoamProjectContext.depositBox.deposit(projectContext)

    try { f(projectContextReceipt) }
    finally {
      LoamProjectContext.depositBox.remove(projectContextReceipt)
    }
  }

  /** Compiles Loam script into execution plan */
  def compile(
      config: LoamConfig, 
      script: LoamScript,
      propertiesForLoamCode: Iterable[HeterogeneousMap.Entry[_, _]]): LoamCompiler.Result = {
    
    compile(LoamProject(config, script), propertiesForLoamCode)
  }

  private def failureDueToException(e: Throwable): LoamCompiler.Result = {
    error(s"${e.getClass.getName} while trying to compile: ${e.getMessage}", e)

    logCompilationErrors(LoamCompiler.Result.throwable(reporter, e))
  }

  /** Compiles Loam script into execution plan */
  def compile(
      project: LoamProject, 
      propertiesForLoamCode: Iterable[HeterogeneousMap.Entry[_, _]]): LoamCompiler.Result = compileLock.synchronized {
    
    depositProjectContextAndThen(project, propertiesForLoamCode) { projectContextReceipt =>
      try {
        val sourceFiles = project.scripts.map(LoamCompiler.toBatchSourceFile(projectContextReceipt))

        TimeUtils.time("Compiling .scala files", debug(_)) {
          withRun { run =>
            run.compileSources(sourceFiles.toList)
          }
        }

        if (targetDirectory.nonEmpty) {
          debug(s"Completed compilation and there were $soManyIssues.")

          val classLoader = new AbstractFileClassLoader(targetDirectory, getClass.getClassLoader)

          val scriptBoxes = TimeUtils.time("Evaluating Loam code", debug(_)) {
            project.scripts.map(LoamCompiler.evaluateLoamScript(classLoader))
          }

          val scriptBox = scriptBoxes.head
          val graph = scriptBox.graph

          if (compilationConfig.shouldValidateGraph) {
            TimeUtils.time("Validating graph", debug(_)) {
              validateGraph(graph)
            }
          }

          reportCompilation(project, graph, projectContextReceipt)

          val projectContext = scriptBox.projectContext

          debug(s"Compilation finished.")

          //If any .class files were produced - meaning we're in this branch - then there were no errors,
          //so we can skip them and say for sure compilation was successful.
          LoamCompiler.Result.success(reporter.warnings, reporter.infos, projectContext)
        } else {
          error(s"Compilation failed. There were $soManyIssues.")

          logCompilationErrors(LoamCompiler.Result.failure(reporter))
        }
      } catch {
        case ReportableCompilationError(e) => failureDueToException(e)
      }
    }
  }
}
