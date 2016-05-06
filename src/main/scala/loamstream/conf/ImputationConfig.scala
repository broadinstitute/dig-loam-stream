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
  val shapeItExecutableKey = s"$shapeItBaseKey.executable"
  val shapeItVcfFileKey = s"$shapeItBaseKey.vcfFile"
  val shapeItMapFileKey = s"$shapeItBaseKey.mapFile"
  val shapeItHapFileKey = s"$shapeItBaseKey.hapFile"
  val shapeItSampleFileKey = s"$shapeItBaseKey.sampleFile"
  val shapeItLogFileKey = s"$shapeItBaseKey.logFile"
  val shapeItNumThreadsKey = s"$shapeItBaseKey.numThreads"

  val shapeItExecutable = props.getString(shapeItExecutableKey).get
  val shapeItVcfFile = props.getString(shapeItVcfFileKey).get
  val shapeItMapFile = props.getString(shapeItMapFileKey).get
  val shapeItHapFile = props.getString(shapeItHapFileKey).get
  val shapeItSampleFile = props.getString(shapeItSampleFileKey).get
  val shapeItLogFile = props.getString(shapeItLogFileKey).get
  val shapeItNumThreads = props.getString(shapeItNumThreadsKey).get
}
