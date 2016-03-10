package loamstream.conf

import com.typesafe.config.Config
import scala.util.Try

/**
 * @author clint
 * date: Mar 10, 2016
 */
final case class TypesafeConfigLproperties(config: Config) extends LProperties {
  import TypesafeConfigLproperties._
  
  override def getAs[A: Extractor](key: String): Option[A] = {
    val extractor = implicitly[Extractor[A]]
    
    val attempt = for {
      fullKey <- Try(qualifiedKey(key))
      configValue <- Try(config.getValue(fullKey))
      extracted <- extractor.extract(configValue)
    } yield extracted
    
    attempt.toOption
  }
}

object TypesafeConfigLproperties {
  private val prefix = "loamstream"
  
  private[conf] def qualifiedKey(k: String): String = {
    require(k != null)
    
    val trimmedKey = k.trim
    
    require(!trimmedKey.isEmpty)
    
    s"$prefix.$trimmedKey"
  }
}