package loamstream.conf

import java.nio.file.Path

import com.typesafe.config.ConfigFactory

/**
  * LoamStream
  * Created by oliverr on 3/4/2016.
  */
trait LProperties {
  def getString(key: String): Option[String]

  def getPath(key: String): Option[Path]
}

object LProperties {
  lazy val Default: LProperties = load("loamstream")

  def load(prefix: String): LProperties =
    TypesafeConfigLproperties(ConfigFactory.load(prefix).withFallback(ConfigFactory.load()))
}
