package loamstream.util

import java.nio.file.{Path, Files => JFiles}

import scala.sys.process.{Process, ProcessBuilder}

/** A class representing a Bash script */
case class BashScript(path: Path) {
  def processBuilder(workDir: Path): ProcessBuilder =
    Process(Seq("sh", BashScript.escapeString(path.toString)), workDir.toFile)
}

object BashScript {

  def fromCommandLineString(string: String,
                            path: Path = JFiles.createTempFile("LoamStreamBashScript", ".sh")): BashScript = {
    Files.writeTo(path)(string + "\nexit\n")
    BashScript(path)
  }

  def escapeString(string: String): String = string // TODO


}
