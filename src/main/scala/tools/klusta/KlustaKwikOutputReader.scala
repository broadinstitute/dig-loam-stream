package tools.klusta

import java.nio.file.Path

import loamstream.util.shot.{Hit, Miss, Shot}

import scala.io.{Codec, Source}

/**
  * LoamStream
  * Created by oliverr on 3/18/2016.
  */
object KlustaKwikOutputReader {

  case class Output(nClusters: Int, clustering: Seq[Int])

  def read(konfig: KlustaKwikKonfig): Shot[Output] = read(konfig.workDir, konfig.fileBase, konfig.iShank)

  def read(workDir: Path, fileBase: String, iShank: Int): Shot[Output] = {
    val fileName = fileBase + ".clu." + iShank
    val file = workDir.resolve(fileName)
    val lineIter = Source.fromFile(file.toFile)(Codec.UTF8).getLines
    if (lineIter.hasNext) {
      try {
        val nClusters = lineIter.next.toInt
        if (lineIter.hasNext) {
          val clustering = lineIter.toSeq.map(_.toInt)
          Hit(Output(nClusters, clustering))
        } else {
          Miss("No clustering entries")
        }
      } catch {
        case ex: NumberFormatException => Miss("Encountered a line that was not an integer: " + ex.toString)
      }
    } else {
      Miss("File has no lines.")
    }
  }

}
