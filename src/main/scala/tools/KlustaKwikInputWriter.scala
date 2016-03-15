package tools

import java.io.PrintStream

/**
  * LoamStream
  * Created by oliverr on 3/15/2016.
  */
object KlustaKwikInputWriter {

  def write(out: PrintStream, data: Seq[Seq[Double]]): Unit = {
    out.println(data.head.size) // scalastyle:ignore
    for (line <- data) {
      out.println(line.mkString("\t")) // scalastyle:ignore
    }
  }

}
