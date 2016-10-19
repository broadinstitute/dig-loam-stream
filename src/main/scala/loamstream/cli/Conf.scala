package loamstream.cli

import java.nio.file.{Path, Paths}

import loamstream.util.Loggable
import org.rogach.scallop._
import org.rogach.scallop.exceptions.ScallopException

/**
 * Provides a command line interface for LoamStream apps
 * using [[https://github.com/scallop/scallop Scallop]] under the hood
 *
 * @param arguments command line arguments provided by the app user
 */
final case class Conf(arguments: Seq[String]) extends ScallopConf(arguments) with Loggable {
  /** Inform the user about expected usage upon erroneous input/behaviour. */
  override def onError(e: Throwable) = e match {
    case ScallopException(message) =>
      error(message)
      printHelp
      sys.exit(1)
    case ex => super.onError(ex)
  }

  /** In the verify stage, check that files with the supplied paths exist. */
  private def validatePathsExist(paths: ScallopOption[List[Path]]): Unit =
    paths.toOption.foreach { paths =>
      paths.foreach { path =>
        if (!path.toFile.exists) {
          error("File at '" + path + "' not found")
          sys.exit(1)
        }
      }
  }

  private def printHelpIfNoArgsAndExit(): Unit =
    if (arguments.isEmpty) {
    printHelp()
    sys.exit(1)
  }

  // TODO: Add version info (ideally from build.sbt)?
  banner("""LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
           |Usage: scala loamstream.jar [Option]...
           |       or if you have an executable produced by SBT Native Packager:
           |       loamstream [Option]...
           |Options:
           |""".stripMargin)

  val listPathConverter = listArgConverter[Path](Paths.get(_))
  val loam = opt[List[Path]](descr = "Path to loam script", required = true, validate = _.nonEmpty)(listPathConverter)
  val conf = opt[Path](descr = "Path to config file", required = true)

  validatePathExists(conf)

  verify()

  // The following checks come after verify() since options are lazily built by Scallop
  printHelpIfNoArgsAndExit()
  validatePathsExist(loam)
}
