package loamstream.util

import java.nio.file.Path
import java.nio.file.{Files => JFiles}

import scala.sys.process.Process
import scala.sys.process.ProcessBuilder

/** A class representing a Bash script */
final case class BashScript(path: Path) {
  def processBuilder(workDir: Path): ProcessBuilder = {
    Process(Seq("sh", BashScript.escapeString(path.toString)), workDir.toFile)
  }
}

object BashScript extends Loggable {

  private def defaultFileName: Path = JFiles.createTempFile("LoamStreamBashScript", ".sh")

  def fromCommandLineString(string: String, path: Path = defaultFileName): BashScript = {
    Files.writeTo(path)(s"${string}\nexit\n")

    debug(s"Wrote shell script to $path for command '$string'")

    BashScript(path)
  }

  /** Characters that should be escaped by prefixing with backslash */
  private val charsToBeEscaped: Set[Char] = Set('\\', '\'', '\"', '\n', '\r', '\t', '\b', '\f', ' ')

  /** 
   * Escapes string for Bash.
   * Note new StringBuilder-oriented approach, which is significantly faster than the previous one that relied on
   * String.flatMap.  Somewhat surprisingly, this method was a significant component of the graph-validation code's
   * running time before switching to the StringBuilder approach.   
   */
  def escapeString(string: String): String = {
    val builder = new StringBuilder(string.length * 2)
    
    string.foreach { c =>
      
      if(charsToBeEscaped(c)) {
        builder.append('\\')
      }
      
      builder.append(c)
    }
    
    builder.toString
  }

  object Implicits {
    implicit class BashPath(val path: Path) extends AnyVal {
      def render: String = escapeString(path.toString.replace('\\', '/'))
    }
  }
}
