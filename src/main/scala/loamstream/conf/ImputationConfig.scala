package loamstream.conf

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
case class ImputationConfig private (shapeItWorkDir: String, shapeItExecutable: String, shapeItScript: String,
                            shapeItVcfFile: String, shapeItMapFile: String, shapeItHapFile: String,
                            shapeItSampleFile: String, shapeItLogFile: String, shapeItNumThreads: Int) {
}

object ImputationConfig {
  val DEFAULT_CONFIG = "loamstream"

  val shapeItBaseKey = "imputation.shapeit"
  val shapeItWorkDirKey = s"$shapeItBaseKey.workDir"
  val shapeItExecutableKey = s"$shapeItBaseKey.executable"
  val shapeItScriptKey = s"$shapeItBaseKey.script"
  val shapeItVcfFileKey = s"$shapeItBaseKey.vcfFile"
  val shapeItMapFileKey = s"$shapeItBaseKey.mapFile"
  val shapeItHapFileKey = s"$shapeItBaseKey.hapFile"
  val shapeItSampleFileKey = s"$shapeItBaseKey.sampleFile"
  val shapeItLogFileKey = s"$shapeItBaseKey.logFile"
  val shapeItNumThreadsKey = s"$shapeItBaseKey.numThreads"

  def apply(configFile: String): ImputationConfig = {
    val config = ConfigFactory.parseFile(new File(configFile)).withFallback(ConfigFactory
      .load(ImputationConfig.DEFAULT_CONFIG))
    ImputationConfig(config)
  }

  def apply(config: Config): ImputationConfig = {
    val shapeItProps = TypesafeConfigLproperties(config)

    val shapeItWorkDir = shapeItProps.getString(ImputationConfig.shapeItWorkDirKey).get
    val shapeItExecutable = shapeItProps.getString(ImputationConfig.shapeItExecutableKey).get
    val shapeItScript = shapeItProps.getString(ImputationConfig.shapeItScriptKey).get
    val shapeItVcfFile = Paths.get(shapeItWorkDir, shapeItProps.getString(ImputationConfig.shapeItVcfFileKey).get)
      .toString
    val shapeItMapFile = Paths.get(shapeItWorkDir, shapeItProps.getString(ImputationConfig.shapeItMapFileKey).get)
      .toString
    val shapeItHapFile = Paths.get(shapeItWorkDir, shapeItProps.getString(ImputationConfig.shapeItHapFileKey).get)
      .toString
    val shapeItSampleFile = Paths.get(shapeItWorkDir, shapeItProps.getString(ImputationConfig.shapeItSampleFileKey).get)
      .toString
    val shapeItLogFile = Paths.get(shapeItWorkDir, shapeItProps.getString(ImputationConfig.shapeItLogFileKey).get)
      .toString
    val shapeItNumThreads = shapeItProps.getString(ImputationConfig.shapeItNumThreadsKey).get.toInt

    ImputationConfig(shapeItWorkDir, shapeItExecutable, shapeItScript, shapeItVcfFile, shapeItMapFile, shapeItHapFile,
      shapeItSampleFile, shapeItLogFile, shapeItNumThreads)
  }
}
