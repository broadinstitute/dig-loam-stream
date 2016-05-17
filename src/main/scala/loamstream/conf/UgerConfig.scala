package loamstream.conf

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
case class UgerConfig(configFile: String) {
  val ugerConfig = ConfigFactory.parseFile(new File(configFile)).withFallback(ConfigFactory.load("loamstream"))
  val ugerProps = TypesafeConfigLproperties(ConfigFactory.load(ugerConfig))
  val ugerBaseKey = "uger"

  val ugerWorkDirKey = s"$ugerBaseKey.workDir"
  val ugerLogFileKey = s"$ugerBaseKey.logFile"

  val ugerWorkDir = ugerProps.getString(ugerWorkDirKey).get
  val ugerLogFile = Paths.get(ugerWorkDir, ugerProps.getString(ugerLogFileKey).get).toString
}
