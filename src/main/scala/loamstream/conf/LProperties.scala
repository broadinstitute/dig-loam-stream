package loamstream.conf

import java.nio.file.Path

import com.typesafe.config.ConfigFactory
import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 3/4/2016.
  */
trait LProperties {
  def tryGetString(key: String): Try[String]

  def tryGetPath(key: String): Try[Path]
  
  def getString(key: String): Option[String] = tryGetString(key).toOption

  def getPath(key: String): Option[Path] = tryGetPath(key).toOption
}

object LProperties {
  val defaultPrefix = "loamstream"
  
  lazy val Default: LProperties = load(defaultPrefix)

  def load(prefix: String): LProperties =
    TypesafeConfigLproperties(ConfigFactory.load(prefix).withFallback(ConfigFactory.load()))
}
