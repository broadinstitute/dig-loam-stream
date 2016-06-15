package loamstream.conf

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import java.nio.file.Path
import scala.util.Try
import loamstream.util.ConfigEnrichments
import java.io.FileNotFoundException
import scala.util.control.NonFatal
import scala.util.Failure
import scala.util.Success

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
final case class UgerConfig private (ugerWorkDir: Path, ugerLogFile: Path)

object UgerConfig extends ConfigCompanion[UgerConfig] {

  object Keys extends TypesafeConfig.KeyHolder("uger") {
    val ugerWorkDirKey = key("workDir")
    val ugerLogFileKey = key("logFile")
  }

  override def fromConfig(config: Config): Try[UgerConfig] = {
    val ugerProps = TypesafeConfigLproperties(config)

    import Keys._
    
    for {
      ugerWorkDir <- ugerProps.tryGetPath(ugerWorkDirKey)
      ugerLogFile <- ugerProps.tryGetPath(ugerLogFileKey)
    } yield {
      UgerConfig(ugerWorkDir, ugerLogFile)
    }
  }
}
