package loamstream.conf

import java.io.File

import com.typesafe.config.ConfigFactory

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
case class ImputationConfig(configFile: String) {
  val imputationConfig = ConfigFactory.parseFile(new File(configFile))
  val props = TypesafeConfigLproperties(ConfigFactory.load(imputationConfig))
  val shapeItBaseKey = "imputation.shapeit"
  val shapeItWorkDirKey = s"$shapeItBaseKey.workDir"
  val shapeItExecutableKey = s"$shapeItBaseKey.executable"
  val shapeItVcfFileKey = s"$shapeItBaseKey.vcfFile"
  val shapeItMapFileKey = s"$shapeItBaseKey.mapFile"
  val shapeItHapFileKey = s"$shapeItBaseKey.hapFile"
  val shapeItSampleFileKey = s"$shapeItBaseKey.sampleFile"
  val shapeItLogFileKey = s"$shapeItBaseKey.logFile"
  val shapeItNumThreadsKey = s"$shapeItBaseKey.numThreads"

  val shapeItWorkDir = props.getString(shapeItWorkDirKey).get
  val shapeItExecutable = props.getString(shapeItExecutableKey).get
  val shapeItVcfFile = shapeItWorkDir + props.getString(shapeItVcfFileKey).get
  val shapeItMapFile = shapeItWorkDir + props.getString(shapeItMapFileKey).get
  val shapeItHapFile = shapeItWorkDir + props.getString(shapeItHapFileKey).get
  val shapeItSampleFile = shapeItWorkDir + props.getString(shapeItSampleFileKey).get
  val shapeItLogFile = shapeItWorkDir + props.getString(shapeItLogFileKey).get
  val shapeItNumThreads = props.getString(shapeItNumThreadsKey).get
}
