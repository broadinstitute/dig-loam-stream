package loamstream.conf

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import java.nio.file.Path
import loamstream.util.PathEnrichments
import scala.util.Try
import loamstream.util.ConfigEnrichments

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
final case class ImputationConfig(shapeIt: ShapeItConfig, impute2: Impute2Config)

object ImputationConfig extends ConfigCompanion[ImputationConfig] {
  
  object Keys extends TypesafeConfig.KeyHolder("loamstream.imputation") {
    val shapeIt = key("shapeit")
    val impute2 = key("impute2")
  }
  
  override def fromConfig(config: Config): Try[ImputationConfig] = {
    
    import ConfigEnrichments._
    import Keys._
    
    for {
      shapeItTsConfig <- config.tryGetConfig(shapeIt)
      shapeItConfig <- ShapeItConfig.fromConfig(shapeItTsConfig)
      impute2TsConfig <- config.tryGetConfig(impute2)
      impute2Config <- Impute2Config.fromConfig(impute2TsConfig)
    } yield {
      ImputationConfig(shapeItConfig, impute2Config)
    }
  }
}
