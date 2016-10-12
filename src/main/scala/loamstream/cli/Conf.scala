package loamstream.cli

import java.nio.file.{Path, Paths}

import org.rogach.scallop._

final case class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  // TODO: Add version info (ideally from build.sbt)?
  banner("""LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
           |Usage: scala loamstream.jar [Option]...
           |Options:
           |""".stripMargin)

  val loam = opt[Path](descr = "Path to loam script")
  val conf = opt[Path](descr = "Path to config file", default = Option(Paths.get("src/main/resources/loamstream.conf")))

  dependsOnAny(conf, List(loam))
  validatePathExists(loam)
  validatePathExists(conf)

  verify()
}

object Conf {
  def apply(): Conf = Conf(Seq("--help"))
}