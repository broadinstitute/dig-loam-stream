package loamstream.conf

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import java.nio.file.Path
import loamstream.util.PathEnrichments

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
final case class ImputationConfig private (
    shapeItWorkDir: Path, 
    shapeItExecutable: Path, 
    shapeItScript: Path,
    shapeItVcfFile: Path, 
    shapeItMapFile: Path, 
    shapeItHapFile: Path,
    shapeItSampleFile: Path, 
    shapeItLogFile: Path, 
    shapeItNumThreads: Int)

object ImputationConfig {
  
  object Keys extends TypesafeConfig.KeyHolder("imputation.shapeit") {
    val workDirKey = key("workDir")
    val executableKey = key("executable")
    val scriptKey = key("script")
    val vcfFileKey = key("vcfFile")
    val mapFileKey = key("mapFile")
    val hapFileKey = key("hapFile")
    val sampleFileKey = key("sampleFile")
    val logFileKey = key("logFile")
    val numThreadsKey = key("numThreads")
  }

  def apply(configFile: String): ImputationConfig = {
    val config = TypesafeConfig.fromFile(configFile)
    
    ImputationConfig(config)
  }

  def apply(config: Config): ImputationConfig = {
    val shapeItProps = TypesafeConfigLproperties(config)

    import Keys._
    import PathEnrichments._
    
    val shapeItWorkDir = Paths.get(shapeItProps.getString(workDirKey).get)
    val shapeItExecutable = Paths.get(shapeItProps.getString(executableKey).get)
    val shapeItScript = Paths.get(shapeItProps.getString(scriptKey).get)
    
    val shapeItVcfFile = shapeItWorkDir / shapeItProps.getString(vcfFileKey).get
    val shapeItMapFile = shapeItWorkDir / shapeItProps.getString(mapFileKey).get
    val shapeItHapFile = shapeItWorkDir / shapeItProps.getString(hapFileKey).get
    val shapeItSampleFile = shapeItWorkDir / shapeItProps.getString(sampleFileKey).get
    val shapeItLogFile = shapeItWorkDir / shapeItProps.getString(logFileKey).get
    val shapeItNumThreads = shapeItProps.getString(numThreadsKey).get.toInt

    ImputationConfig(
        shapeItWorkDir, 
        shapeItExecutable, 
        shapeItScript, 
        shapeItVcfFile, 
        shapeItMapFile, 
        shapeItHapFile,
        shapeItSampleFile, 
        shapeItLogFile, 
        shapeItNumThreads)
  }
}
