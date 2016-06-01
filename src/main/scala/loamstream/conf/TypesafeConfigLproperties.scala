package loamstream.conf

import java.nio.file.{Path, Paths}

import com.typesafe.config.Config

import scala.util.Try
import loamstream.util.ConfigEnrichments

/**
  * @author clint
  *         date: Mar 10, 2016
  */
final case class TypesafeConfigLproperties(config: Config) extends LProperties {

  import TypesafeConfigLproperties._
  import ConfigEnrichments._

  override def tryGetString(key: String): Try[String] = {
    for {
      fullKey <- Try(qualifiedKey(key))
      s <- config.tryGetString(fullKey)
    } yield s
  }

  override def tryGetPath(key: String): Try[Path] = tryGetString(key).map(Paths.get(_))
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