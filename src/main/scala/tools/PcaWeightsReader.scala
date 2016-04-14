package tools

import java.nio.file.Path

import scala.io.Source
import scala.util.Try

import loamstream.conf.LProperties
import loamstream.util.LoamFileUtils
import loamstream.util.Loggable
import loamstream.util.StringUtils


/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 3/8/16.
 */
object PcaWeightsReader extends Loggable {

  private[tools] val weightsFilePropertiesKey = "pca.weights.file"

  def weightsFilePath(conf: LProperties): Option[Path] = {
    conf.getString(weightsFilePropertiesKey).map(LoamFileUtils.resolveRelativePath)
  }

  def read(path: Path): Map[String, Seq[Double]] = LoamFileUtils.enclosed(Source.fromFile(path.toFile))(read)

  def read(source: Source): Map[String, Seq[Double]] = {
    
    def parse(line: String): Try[(String, Seq[Double])] = Try {
      import StringUtils.IsLong
      
      val Array(id, IsLong(_), IsLong(_), rest @ _*) = line.trim.split("\\s+")

      val weights = rest.map(_.toDouble)

      id -> weights
    }

    val mappings = for {
      line <- source.getLines
      if StringUtils.isNotWhitespace(line)
      //TODO: This just ignores parsing failures; should it fail loudly, log, or something else?
      mapping <- parse(line).toOption
    } yield {
      mapping
    }

    mappings.toMap
  }
}
