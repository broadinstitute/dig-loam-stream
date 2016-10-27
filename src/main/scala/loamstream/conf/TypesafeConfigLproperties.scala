package loamstream.conf

import com.typesafe.config.Config
import loamstream.util.ConfigEnrichments

import scala.util.Try

/**
  * @author clint
  *         date: Mar 10, 2016
  */
final case class TypesafeConfigLproperties(config: Config) extends LProperties {

  import ConfigEnrichments._
  import TypesafeConfigLproperties._

  override def tryGetString(key: String): Try[String] = {
    for {
      fullKey <- Try(qualifiedKey(key))
      s <- config.tryGetString(fullKey)
    } yield s
  }
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