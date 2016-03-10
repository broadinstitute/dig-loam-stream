package loamstream.conf

import java.io.FileInputStream
import java.nio.file.{DirectoryStream, Files, Path}
import java.util.Properties

import utils.FileUtils

import scala.collection.JavaConverters.asScalaIteratorConverter
import com.typesafe.config.ConfigFactory

/**
  * LoamStream
  * Created by oliverr on 3/4/2016.
  */
trait LProperties {
  def getAs[A: Extractor](key: String): Option[A]
}

object LProperties {
  lazy val Default: LProperties = load("loamstream")
  
  def load(prefix: String): LProperties = TypesafeConfigLproperties(ConfigFactory.load(prefix).withFallback(ConfigFactory.load()))
}
