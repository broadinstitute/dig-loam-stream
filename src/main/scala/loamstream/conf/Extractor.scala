package loamstream.conf

import com.typesafe.config.Config
import scala.util.Try
import com.typesafe.config.ConfigValue
import java.nio.file.Path
import scala.util.Success
import scala.util.Failure
import utils.Tries
import java.nio.file.Paths

/**
 * @author clint
 * date: Mar 10, 2016
 */
trait Extractor[A] {
  def extract(configValue: ConfigValue): Try[A]
}

object Extractor {
  implicit object StringExtractor extends Extractor[String] {
    override def extract(configValue: ConfigValue): Try[String] = {
      configValue.unwrapped match {
        case s: String => Success(s)
        case _ => Tries.failure("Couldn't extract a String from config value $configValue")
      }
    }
  }
  
  implicit object PathExtractor extends Extractor[Path] {
    override def extract(configValue: ConfigValue): Try[Path] = {
      StringExtractor.extract(configValue).map(Paths.get(_))
    }
  }
}