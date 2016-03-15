package tools

import java.nio.file.Path

import loamstream.conf.LProperties
import utils.{FileUtils, Loggable}

import scala.io.Source

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 3/8/16.
  */
object PcaWeightsReader extends Loggable {

  val weightsFilePropertiesKey = "pca.weights.file"

  def weightsFilePath: Option[Path] =
    LProperties.Default.getString(weightsFilePropertiesKey).flatMap(FileUtils.resolveRelativePath)

  def read(path: Path): Map[String, Seq[Double]] = {
    var weightsMap = Map.empty[String, Seq[Double]]
    for (line <- Source.fromFile(path.toFile).getLines()) {
      val tokens: Seq[String] = line.trim.split("\\s+").toSeq
      val id = tokens.head
      val weights = tokens.drop(3).map(_.toDouble)
      weightsMap += (id -> weights)
    }
    weightsMap
  }

}
