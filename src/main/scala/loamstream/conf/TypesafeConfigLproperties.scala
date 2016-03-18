package loamstream.conf

import java.nio.file.{Path, Paths}

import com.typesafe.config.Config

import scala.util.Try

/**
  * @author clint
  *         date: Mar 10, 2016
  */
final case class TypesafeConfigLproperties(config: Config) extends LProperties {

  import TypesafeConfigLproperties._

  override def getString(key: String): Option[String] = {
    val attempt = for {
      fullKey <- Try(qualifiedKey(key))
      s <- Try(config.getString(fullKey))
    } yield s

    attempt.toOption
  }

  override def getPath(key: String): Option[Path] = getString(key).map(Paths.get(_))
}

object TypesafeConfigLproperties {
  private val prefix = "loamstream"

  private[conf] def qualifiedKey(k: String): String = {
    require(k != null) // scalastyle:ignore null

    val trimmedKey = k.trim

    require(!trimmedKey.isEmpty)

    s"$prefix.$trimmedKey"
  }
}