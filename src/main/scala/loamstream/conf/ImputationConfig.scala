package loamstream.conf

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.ConfigFactory

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
case class ImputationConfig(configFile: String) {
  val imputationConfig = ConfigFactory.parseFile(new File(configFile)).withFallback(ConfigFactory.load("loamstream"))
  val shapeItProps = TypesafeConfigLproperties(ConfigFactory.load(imputationConfig))
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

  val shapeItWorkDir = shapeItProps.getString(shapeItWorkDirKey).get
  val shapeItExecutable = shapeItProps.getString(shapeItExecutableKey).get
  val shapeItScript = shapeItProps.getString(shapeItScriptKey).get
  val shapeItVcfFile = Paths.get(shapeItWorkDir, shapeItProps.getString(shapeItVcfFileKey).get).toString
  val shapeItMapFile = Paths.get(shapeItWorkDir, shapeItProps.getString(shapeItMapFileKey).get).toString
  val shapeItHapFile = Paths.get(shapeItWorkDir, shapeItProps.getString(shapeItHapFileKey).get).toString
  val shapeItSampleFile = Paths.get(shapeItWorkDir, shapeItProps.getString(shapeItSampleFileKey).get).toString
  val shapeItLogFile = Paths.get(shapeItWorkDir, shapeItProps.getString(shapeItLogFileKey).get).toString
  val shapeItNumThreads = shapeItProps.getString(shapeItNumThreadsKey).get
}
