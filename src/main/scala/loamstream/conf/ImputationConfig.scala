package loamstream.conf

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import java.nio.file.Path

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
  val DefaultConfigPrefix = "loamstream"

  object Keys {
    private val base = "imputation.shapeit"
  
    val workDirKey = s"$base.workDir"
    val executableKey = s"$base.executable"
    val scriptKey = s"$base.script"
    val vcfFileKey = s"$base.vcfFile"
    val mapFileKey = s"$base.mapFile"
    val hapFileKey = s"$base.hapFile"
    val sampleFileKey = s"$base.sampleFile"
    val logFileKey = s"$base.logFile"
    val numThreadsKey = s"$base.numThreads"
  }

  def apply(configFile: String): ImputationConfig = {
    val defaultConfig = ConfigFactory.load(DefaultConfigPrefix)
    
    val config = ConfigFactory.parseFile(new File(configFile)).withFallback(defaultConfig)
    
    ImputationConfig(config)
  }

  private[this] final implicit class PathHelpers(val path: Path) extends AnyVal {
    def /(next: String): Path = Paths.get(path.toString, next)
  }
  
  def apply(config: Config): ImputationConfig = {
    val shapeItProps = TypesafeConfigLproperties(config)

    import Keys._
    
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
