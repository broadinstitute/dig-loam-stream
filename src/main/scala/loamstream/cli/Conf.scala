package loamstream.cli

import java.nio.file.Path
import java.nio.file.Paths

import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop._

import loamstream.util.Loggable
import loamstream.util.Versions

/**
 * Provides a command line interface for LoamStream apps
 * using [[https://github.com/scallop/scallop Scallop]] under the hood
 *
 * @param arguments command line arguments provided by the app user
 * @param exitTheJvmOnValidationError whether or not to exit the whole JVM on any validation errors; setting this
 * to false is useful for tests, so that a validation failure doesn't make SBT exit.  If this is false, a 
 * CliException is thrown instead of invoking 'sys.exit()'.
 */
final case class Conf(
    arguments: Seq[String], 
    exitTheJvmOnValidationError: Boolean = true) extends ScallopConf(arguments) with Loggable {
  
  /** Inform the user about expected usage upon erroneous input/behaviour. */
  override def onError(e: Throwable): Unit = e match {
    case ScallopException(message) =>
      error(message)
      printHelp()
      exitOrThrow(message)
    case ex => super.onError(ex)
  }

  /** In the verify stage, check that files with the supplied paths exist. */
  private def validatePathsExist(paths: ScallopOption[List[Path]]): Unit = {
    paths.toOption.foreach { paths =>
      paths.foreach { path =>
        if (!path.toFile.exists) {
          val msg = s"File at '$path' not found"
          
          error(msg)
          exitOrThrow(msg)
        }
      }
    }
  }

  private def printHelpIfNoArgsAndExit(): Unit = {
    if (arguments.isEmpty) {
      printHelp()
      exitOrThrow("No arguments provided")
    }
  }
  
  private def printVersionInfoAndExitIfNeeded(): Unit = {
    if (version()) {
      println(s"${Versions.load().get.toString}") //scalastyle:ignore regex
      exitOrThrow("")
    }
  }
  
  private def exitOrThrow(msg: String): Unit = {
    if(exitTheJvmOnValidationError) {
      sys.exit(1)
    } else {
      throw new CliException(msg)
    }
  }

  // TODO: Add version info (ideally from build.sbt)?
  banner("""LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
           |Usage: scala loamstream.jar [Option]...
           |       or if you have an executable produced by SBT Native Packager:
           |       loamstream [Option]...
           |Options:
           |""".stripMargin)

  //Using all default args for `opt` makes it a flag 
  val version: ScallopOption[Boolean] = opt[Boolean]()
           
  val loam: ScallopOption[List[Path]] = {
    val listPathConverter: ValueConverter[List[Path]] = listArgConverter[Path](Paths.get(_))
    
    opt[List[Path]](descr = "Path to loam script", validate = _.nonEmpty)(listPathConverter)
  }
  
  val conf: ScallopOption[Path] = opt[Path](descr = "Path to config file")

  //NB: Tell Scallop that if --version is supplied, --loam and --conf are not required, and that if --version
  //is NOT supplied, then BOTH --loam and --conf must be supplied.  These calls replace the `required` param to `opt`
  //for `loam` and `conf`
  conflicts(version, List(loam, conf))
  codependent(loam, conf)
  

  //NB: This needs to come before the call to verify(), or else we don't fail properly when --conf is omitted. Shrug. 
  validatePathExists(conf)
  
  verify()
  
  // The following checks come after verify() since options are lazily built by Scallop
  printHelpIfNoArgsAndExit()
  printVersionInfoAndExitIfNeeded()
  
  validatePathsExist(loam)
}
