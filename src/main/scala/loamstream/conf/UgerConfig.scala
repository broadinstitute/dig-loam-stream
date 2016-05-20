package loamstream.conf

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
case class UgerConfig private (ugerWorDir: String, ugerLogFile: String) {
}

object UgerConfig {
  val DEFAULT_CONFIG = "loamstream"

  val ugerBaseKey = "uger"
  val ugerWorkDirKey = s"$ugerBaseKey.workDir"
  val ugerLogFileKey = s"$ugerBaseKey.logFile"

  def apply(configFile: String): UgerConfig = {
    val config = ConfigFactory.parseFile(new File(configFile)).withFallback(ConfigFactory
      .load(UgerConfig.DEFAULT_CONFIG))
    UgerConfig(config)
  }

  def apply(config: Config): UgerConfig = {
    val ugerProps = TypesafeConfigLproperties(config)

    val ugerWorkDir = ugerProps.getString(ugerWorkDirKey).get
    val ugerLogFile = Paths.get(ugerWorkDir, ugerProps.getString(ugerLogFileKey).get).toString

    UgerConfig(ugerWorkDir, ugerLogFile)
  }
}
