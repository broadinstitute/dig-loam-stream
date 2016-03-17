package tools

import java.io.{File, FileOutputStream, PrintStream}

/**
  * LoamStream
  * Created by oliverr on 3/15/2016.
  */
object KlustaKwikInputWriter {

  def writeFeatures(dir: File, fileBase: String, shankNo: Int, data: Seq[Seq[Double]]): Unit = {
    val fileName = fileBase + ".fet." + shankNo
    val out = new PrintStream(new FileOutputStream(new File(dir, fileName)))
    writeFeatures(out, data)
  }

  def writeFeatures(out: PrintStream, data: Seq[Seq[Double]]): Unit = {
    out.println(data.head.size) // scalastyle:ignore
    for (line <- data) {
      out.println(line.mkString("\t")) // scalastyle:ignore
    }
  }

}
