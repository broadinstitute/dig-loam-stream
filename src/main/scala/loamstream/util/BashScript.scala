package loamstream.util

import java.nio.file.{Path, Files => JFiles}

import scala.sys.process.{Process, ProcessBuilder}

/** A class representing a Bash script */
final case class BashScript(path: Path) {
  def processBuilder(workDir: Path): ProcessBuilder =
    Process(Seq("sh", BashScript.escapeString(path.toString)), workDir.toFile)
}

object BashScript {

  private def defaultFileName: Path = JFiles.createTempFile("LoamStreamBashScript", ".sh")  
  
  def fromCommandLineString(string: String, path: Path = defaultFileName): BashScript = {
    Files.writeTo(path)(string + "\nexit\n")
    BashScript(path)
  }

  /** Characters that should be escaped by prefixing with backslash */
  val charsToBeEscaped: Set[Char] = Set('\\', '$', '\'', '\"', '\n', '\r', '\t', '\b', '\f', ' ')

  /** Escapes string for Bash. */
  def escapeString(string: String): String = string.flatMap {
    case c if charsToBeEscaped(c) => Seq('\\', c)
    case c => Seq(c)
  }
}
