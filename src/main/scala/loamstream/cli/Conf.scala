package loamstream.cli

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import org.rogach.scallop.listArgConverter
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.ValueConverter
import org.rogach.scallop.exceptions.ScallopException

import loamstream.drm.DrmSystem
import loamstream.util.IoUtils
import loamstream.util.Loggable

/**
 * Provides a command line interface for LoamStream apps
 * using [[https://github.com/scallop/scallop Scallop]] under the hood
 *
 * @param arguments command line arguments provided by the app user
 * @param exitTheJvmOnValidationError whether or not to exit the whole JVM on any validation errors; setting this
 * to false is useful for tests, so that a validation failure doesn't make SBT exit.  If this is false, a 
 * CliException is thrown instead of invoking 'sys.exit()'.
 */
final case class Conf(arguments: Seq[String]) extends ScallopConf(arguments) with Loggable {
  
  def printHelp(message: String): Unit = {
    printHelp()
    
    IoUtils.printToConsole()
    
    IoUtils.printToConsole(message)
  }
  
  /** Inform the user about expected usage upon erroneous input/behaviour. */
  override def onError(e: Throwable): Unit = e match {
    case e @ ScallopException(message) => {
      printHelp(message)
      
      throw new CliException(e)
    }
    case ex => super.onError(ex)
  }

  private def printHelpIfNoArgsAndExit(): Unit = {
    if (arguments.isEmpty) {
      printHelp()
    }
  }

  private val listPathConverter: ValueConverter[Seq[Path]] = listArgConverter[Path](Paths.get(_)).map(_.toSeq)
  
  // TODO: Add version info (ideally from build.sbt)?
  banner("""LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
           |Usage: scala loamstream.jar [options] [loam file(s)]
           |       or if you have an executable produced by SBT Native Packager:
           |       loamstream [options] [loam file(s)]
           |Options:
           |""".stripMargin)
  
           
  /** Whether to show version info and quit */         
  val version: ScallopOption[Boolean] = opt[Boolean](descr = "Print version information and exit")
  
  /** Whether to show usage info and quit */
  val help: ScallopOption[Boolean] = opt[Boolean](descr = "Show help and exit")
  
  /** Whether to clear the DB */
  val cleanDb: ScallopOption[Boolean] = opt[Boolean](descr = "Clean db")
  
  /** Whether to clear the logs */
  val cleanLogs: ScallopOption[Boolean] = opt[Boolean](descr = "Clean logs")
  
  /** Whether to clear DRM scripts */
  val cleanScripts: ScallopOption[Boolean] = opt[Boolean](descr = "Clean generated DRM (Uger, LSF) scripts")
  
  val clean: ScallopOption[Boolean] = opt[Boolean](
      descr = "Deletes .loamstream/ ; Effectively the same as using all of " +
              "--clean-db, --clean-logs, and --clean-scripts")
  
  val run: ScallopOption[List[String]] = opt(
      descr = 
        "How to run jobs: " +
        "<everything> - always run jobs, never skip them; " +
        "<withAnyMissingOutputs> - run jobs if at least one of their outputs are missing; ignore mod time and hash; " +
        "<allOf> <regexes> - run jobs if their names match ALL of the passed regexes; " +
        "<anyOf> <regexes> - run jobs if their names match ANY of the passed regexes; " +
        "<noneOf> <regexes> - run jobs if their names match NONE of the passed regexes",
      required = false)
  
  val noValidation: ScallopOption[Boolean] = opt[Boolean](
      descr = "Don't validate the graph produced by evaluating .loam files")
      
  /** 
   *  Compile loam files (evaluate them to produce a LoamGraph, or print compilation errors), 
   *  but not execute the jobs declared in them 
   */
  val compileOnly: ScallopOption[Boolean] = opt[Boolean](
      descr = "Only compile the supplied .loam files, don't run them")
      
  /** 
   *  Print a list of what jobs would be executed, without executing anything
   */
  val dryRun: ScallopOption[Boolean] = opt[Boolean](
      descr = "Show what commands would be run without running them")

  /**
   *  Path to LoamStream's config file
   */
  val conf: ScallopOption[Path] = opt[Path](descr = "Path to config file")

  /**
   *  The .loam files to evaluate
   */
  val loams: ScallopOption[Seq[Path]] = opt[Seq[Path]](
      descr = "Path(s) to loam script(s)",
      required = false,
      validate = _.nonEmpty)(listPathConverter)
      
  /**
   * Look up information about a job that produced a path or URI
   */
  val lookup: ScallopOption[Either[Path, URI]] = opt[Either[Path, URI]](
      descr = "Path to output file or URI; look up info on command that made it",
      required = false)(ValueConverters.PathOrGoogleUriConverter)

  /**
   * Don't hash job outputs when determining whether jobs may be skipped
   */
  val disableHashing: ScallopOption[Boolean] = opt[Boolean](
      descr = "Don't hash files when determining whether a job may be skipped.",
      required = false)
  
  /**
   * The DRM backend to use, wither 'lsf' or 'uger'
   */
  val backend: ScallopOption[String] = {
    opt(descr = s"The backend to run jobs on. Options are: ${DrmSystem.values.map(_.name).mkString(",")}")
  }
  
  //NB: Makes Scallop behave
  val trailing: ScallopOption[List[String]] = trailArg(required = false)
  
  //NB: Required by Scallop
  verify()
  
  def toValues: Conf.Values = {
    def getRun: Option[(String, Seq[String])] = run.toOption.flatMap {
      case head +: rest => Some(head -> rest)
      case _            => None
    }
      
    Conf.Values(
      loams = loams.toOption.toSeq.flatten,
      lookup = lookup.toOption,
      cleanDbSupplied = cleanDb.isSupplied || clean.isSupplied,
      cleanLogsSupplied = cleanLogs.isSupplied || clean.isSupplied,
      cleanScriptsSupplied = cleanScripts.isSupplied || clean.isSupplied,
      conf = conf.toOption,
      helpSupplied = help.isSupplied,
      versionSupplied = version.isSupplied,
      noValidationSupplied = noValidation.isSupplied,
      compileOnlySupplied = compileOnly.isSupplied,
      dryRunSupplied = dryRun.isSupplied,
      disableHashingSupplied = disableHashing.isSupplied,
      backend = backend.toOption,
      run = getRun,
      this)
  }
}

object Conf {
  
  object RunStrategies {
    val Everything = "everything"
    val WithAnyMissingOutputs = "withAnyMissingOutputs"
    val AllOf = "allOf"
    val AnyOf = "anyOf"
    val NoneOf = "noneOf"
  }
  
  final case class Values(
      loams: Seq[Path],
      lookup: Option[Either[Path, URI]],
      cleanDbSupplied: Boolean,
      cleanLogsSupplied: Boolean,
      cleanScriptsSupplied: Boolean,
      conf: Option[Path],
      helpSupplied: Boolean,
      versionSupplied: Boolean,
      noValidationSupplied: Boolean,
      compileOnlySupplied: Boolean,
      dryRunSupplied: Boolean,
      disableHashingSupplied: Boolean,
      backend: Option[String],
      run: Option[(String, Seq[String])],
      derivedFrom: Conf) {
    
    def lookupSupplied: Boolean = lookup.isDefined
    
    def confSupplied: Boolean = conf.isDefined
  }
}
