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

/** The compiler compiling Loam scripts into execution plans */
object LoamCompiler extends Loggable {

  object Settings {
    val default: Settings = Settings(logCode = false, logCodeOnError = true)
  }

  final case class Settings(logCode: Boolean, logCodeOnError: Boolean) {
    def logCodeForLevel(level: Loggable.Level.Value): Boolean =
      logCode || (logCodeOnError && (level >= Loggable.Level.warn))
  }

  /** A reporter receiving messages form the underlying Scala compiler
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
    
    private[compiler] def toGraphSource(contextOption: Option[LoamProjectContext]): GraphSource = {
      contextOption.map(_.graphQueue).map(GraphSource.fromQueue).getOrElse(GraphSource.Empty)
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
  final case class Result(
      errors: Seq[Issue], 
      warnings: Seq[Issue], 
      infos: Seq[Issue],
      contextOpt: Option[LoamProjectContext], 
      exOpt: Option[Throwable] = None) {
    
    val graphSource: GraphSource = Result.toGraphSource(contextOpt)
    
    def humanReadableErrors: Seq[CompilationError] = errors.map(CompilationError.from)
    
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
    //NB: Errors are handled specially elsewhere
    def report: String = (summary +: (warnings ++ infos).map(_.summary)).mkString(System.lineSeparator)
  }

  def default: LoamCompiler = apply(Settings.default)

  def apply(settings: Settings): LoamCompiler = new LoamCompiler(settings)
  
  private def makeScalaCompilerSettings(targetDir: VirtualDirectory): ScalaCompilerSettings = {
    val scalaCompilerSettings = new ScalaCompilerSettings(s => ()/*throw new Exception(s)*/)
    
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
final class LoamCompiler(settings: LoamCompiler.Settings = LoamCompiler.Settings.default) extends Loggable {

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
      logLevel: Loggable.Level.Value, 
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
    
      for(graphIssue <- graphIssues) {
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
    
    logScripts(Loggable.Level.trace, project, graphBoxReceipt)
    
    trace(s"""|[Start Graph]
              |${graphPrinter.print(graph)}
              |[End Graph]""".stripMargin)
  }

  private def withRun[A](f: compiler.Run => A): A = {
    reporter.reset()
    targetDirectory.clear()
    
    f(new compiler.Run)
  }
  
  private def depositProjectContextAndThen[A](project: LoamProject)(f: DepositBox.Receipt => A): A = {
    val projectContextReceipt = LoamProjectContext.depositBox.deposit(LoamProjectContext.empty(project.config))
    
    try {  f(projectContextReceipt) }
    finally {
      LoamProjectContext.depositBox.remove(projectContextReceipt)
    }
  }

  /** Compiles Loam script into execution plan */
  def compile(config: LoamConfig, script: LoamScript): LoamCompiler.Result = compile(LoamProject(config, script))
  
  private def failureDueToException(e: Throwable): LoamCompiler.Result = {
    error(s"${e.getClass.getName} while trying to compile: ${e.getMessage}")
          
    logCompilationErrors(LoamCompiler.Result.throwable(reporter, e))
  }
  
  /**
   * In some cases, scalac will write directly to stderr (sigh).  This pollutes the console with long stack traces
   * due to MatchErrors (etc) from deep in the guts of scalac when compiling some loam code with errors.  For 
   * example, a .loam file containing   
   */
  private def suppressStdErr[A](f: => A): A = {
    val oldErr = System.err
    try {
      System.setErr(new PrintStream(new FileOutputStream(".scalac-errors")))
      
      f 
    } finally {
      System.setErr(oldErr)
    }
  }
  
  /** Compiles Loam script into execution plan */
  def compile(project: LoamProject): LoamCompiler.Result = compileLock.synchronized {
    depositProjectContextAndThen(project) { projectContextReceipt =>
      try {
        val sourceFiles = project.scripts.map(LoamCompiler.toBatchSourceFile(projectContextReceipt))
        
        withRun { run =>
          run.compileSources(sourceFiles.toList)
        }
        
        if (targetDirectory.nonEmpty) {
          debug(s"Completed compilation and there were $soManyIssues.")
          
          val classLoader = new AbstractFileClassLoader(targetDirectory, getClass.getClassLoader)
          
          val scriptBoxes = project.scripts.map(LoamCompiler.evaluateLoamScript(classLoader))
          
          val scriptBox = scriptBoxes.head
          val graph = scriptBox.graph
          
          validateGraph(graph)
          
          reportCompilation(project, graph, projectContextReceipt)
          
          val projectContext = scriptBox.projectContext
          
          projectContext.registerGraphSoFar()
          
          LoamCompiler.Result.success(reporter, projectContext)
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
