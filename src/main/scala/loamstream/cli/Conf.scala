package loamstream.cli

import java.nio.file.{Path, Paths}

import loamstream.util.Loggable
import org.rogach.scallop._
import org.rogach.scallop.exceptions.ScallopException

/**
 * Provides a command line interface for LoamStream apps
 * using [[https://github.com/scallop/scallop Scallop]] under the hood
 * @param arguments command line arguments provided by the app user
 */
final case class Conf(arguments: Seq[String]) extends ScallopConf(arguments) with Loggable {
  // Inform the user about expected usage upon erroneous input/behaviour
  override def onError(e: Throwable) = e match {
    case ScallopException(message) =>
      error(message)
      printHelp
      sys.exit(1)
    case ex => super.onError(ex)
  }

  // TODO: Add version info (ideally from build.sbt)?
  banner("""LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
           |Usage: scala loamstream.jar [Option]...
           |       or if you have an executable produced by SBT Native Packager:
           |       loamstream [Option]...
           |Options:
           |""".stripMargin)

  val loam = opt[Path](descr = "Path to loam script")
  val conf = opt[Path](descr = "Path to config file", default = Option(Paths.get("src/main/resources/loamstream.conf")))

  dependsOnAny(conf, List(loam))
  validatePathExists(loam)
  validatePathExists(conf)

  verify()

  if (arguments.isEmpty) {
    printHelp()
    sys.exit(1)
  }
}
