package loamstream.util

import java.nio.file.Path
import java.nio.file.{Files => JFiles}

import scala.sys.process.Process
import scala.sys.process.ProcessBuilder

/** A class representing a Bash script */
final case class BashScript(path: Path) {
  def commandTokens: Seq[String] = Seq("sh", BashScript.escapeString(path.toString))
  
  def processBuilder(workDir: Path): ProcessBuilder = Process(commandTokens, workDir.toFile)
}

object BashScript extends Loggable {

  private def defaultFileName: Path = JFiles.createTempFile("LoamStreamBashScript", ".sh")

  def fromCommandLineString(string: String, path: Path = defaultFileName): BashScript = {
    Files.writeTo(path)(s"${string}\nexit\n")

    debug(s"Wrote shell script to $path for command '$string'")

    BashScript(path)
  }

  /** Characters that should be escaped by prefixing with backslash */
  //NB: Profiler-guided optimization: pattern-matching is almost 2x faster than using a Set, and this method gets 
  //called _a lot_.
  private def shouldBeEscaped(ch: Char): Boolean = ch match {
    case '\\' | '\'' | '\"' | '\n' | '\r' | '\t' | '\b' | '\f' | ' ' => true
    case _ => false
  }

  /**
   * Escapes string for Bash.
   */
  def escapeString(string: String): String = {
    /*
     * Note new StringBuilder-oriented approach, which is significantly faster than the previous one that relied on
     * String.flatMap.  Somewhat surprisingly, this method was a significant component of the graph-validation code's
     * running time before switching to the StringBuilder approach. 
     */
    val builder = new StringBuilder(string.length * 2)

    string.foreach { c =>
      if (shouldBeEscaped(c)) {
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
