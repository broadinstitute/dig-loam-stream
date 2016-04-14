package loamstream.tools.klusta

import java.io.PrintStream
import java.nio.file.{Files, Path}

/**
  * LoamStream
  * Created by oliverr on 3/15/2016.
  */
object KlustaKwikInputWriter {

  def writeFeatures(konfig: KlustaKwikKonfig, data: Seq[Seq[Double]]): Unit =
    writeFeatures(konfig.workDir, konfig.fileBase, konfig.iShank, data)

  def writeFeatures(workDir: Path, fileBase: String, iShank: Int, data: Seq[Seq[Double]]): Unit = {
    val fileName = fileBase + ".fet." + iShank
    val file = workDir.resolve(fileName)
    writeFeatures(file, data)
  }

  def writeFeatures(file: Path, data: Seq[Seq[Double]]): Unit = {
    val out = new PrintStream(Files.newOutputStream(file))
    writeFeatures(out, data)
  }

  def writeFeatures(out: PrintStream, data: Seq[Seq[Double]]): Unit = {
    out.println(data.head.size) // scalastyle:ignore regex
    for (line <- data) {
      out.println(line.mkString("\t")) // scalastyle:ignore regex
    }
  }

}
