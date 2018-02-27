package loamstream.cli

import java.nio.file.Path
import java.nio.file.Paths

import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop._

import loamstream.util.Loggable
import loamstream.util.Versions
import java.net.URI
import loamstream.model.execute.HashingStrategy

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
    
    println() //scalastyle:ignore regex
    
    println(message) //scalastyle:ignore regex
  }
  
  /** Inform the user about expected usage upon erroneous input/behaviour. */
  override def onError(e: Throwable): Unit = e match {
    case ScallopException(message) => {
      error(message)
      printHelp()
    }
    case ex => super.onError(ex)
  }

  private def printHelpIfNoArgsAndExit(): Unit = {
    if (arguments.isEmpty) {
      printHelp()
    }
  }
  
  private def printVersionInfoAndExitIfNeeded(): Unit = {
    if (version()) {
      println(s"${Versions.load().get.toString}") //scalastyle:ignore regex
    }
  }

  private val listPathConverter: ValueConverter[List[Path]] = listArgConverter[Path](Paths.get(_))
  
  // TODO: Add version info (ideally from build.sbt)?
  banner("""LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
           |Usage: scala loamstream.jar [options] [loam file(s)]
           |       or if you have an executable produced by SBT Native Packager:
           |       loamstream [options] [loam file(s)]
           |Options:
           |""".stripMargin)
  
           
  //Using all default args for `opt` makes it a flag 
  val version: ScallopOption[Boolean] = opt[Boolean](descr = "Print version information and exit")
  
  //Using all default args for `opt` makes it a flag 
  val help: ScallopOption[Boolean] = opt[Boolean](descr = "Show help and exit")
  
  //Using all default args for `opt` makes it a flag 
  val runEverything: ScallopOption[Boolean] = opt[Boolean](
      descr = "Run every step in the pipeline, even if they've already been run")
  
  //Using all default args for `opt` makes it a flag 
  val compileOnly: ScallopOption[Boolean] = opt[Boolean](
      descr = "Only compile the supplied .loam files, don't run them")
      
  //Using all default args for `opt` makes it a flag 
  val dryRun: ScallopOption[Boolean] = opt[Boolean](
      descr = "Show what commands would be run without running them")

  val conf: ScallopOption[Path] = opt[Path](descr = "Path to config file")

  val loams: ScallopOption[List[Path]] = trailArg[List[Path]](
      descr = "Path(s) to loam script(s)",
      required = false,
      validate = _.nonEmpty)(listPathConverter)

  val lookup: ScallopOption[Either[Path, URI]] = opt[Either[Path, URI]](
      descr = "Path to output file or URI; look up info on command that made it",
      required = false)(ValueConverters.PathOrGoogleUriConverter)
      
  val disableHashing: ScallopOption[Boolean] = opt[Boolean](
      descr = "Don't hash files when determining whether a job may be skipped.",
      required = false)
      
  //NB: Required by Scallop
  verify()
  
  def toValues: Conf.Values = Conf.Values(
      loams = loams.toOption.toSeq.flatten,
      lookup = lookup.toOption,
      conf = conf.toOption,
      helpSupplied = help.isSupplied,
      versionSupplied = version.isSupplied,
      runEverythingSupplied = runEverything.isSupplied,
      compileOnlySupplied = compileOnly.isSupplied,
      dryRunSupplied = dryRun.isSupplied,
      disableHashingSupplied = disableHashing.isSupplied)
}

object Conf {
  final case class Values(
      loams: Seq[Path],
      lookup: Option[Either[Path, URI]],
      conf: Option[Path],
      helpSupplied: Boolean,
      versionSupplied: Boolean,
      runEverythingSupplied: Boolean,
      compileOnlySupplied: Boolean,
      dryRunSupplied: Boolean,
      disableHashingSupplied: Boolean) {
    
    def lookupSupplied: Boolean = lookup.isDefined
    
    def confSupplied: Boolean = conf.isDefined
  }
}
