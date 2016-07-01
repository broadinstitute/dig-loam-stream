package loamstream.conf

import java.nio.file.Path

import scala.util.Try

import com.typesafe.config.Config

import loamstream.util.PathEnrichments
import loamstream.util.ConfigEnrichments

/**
 * @author clint
 * @author kaan
 * date: Jun 13, 2016
 */
final case class ShapeItConfig(
                                workDir: Path,
                                executable: Path,
                                script: Path,
                                vcfFile: Path,
                                mapFile: Path,
                                sampleFile: Path,
                                hapFile: Path,
                                logFile: Path,
                                numThreads: Int)
    
object ShapeItConfig extends ConfigCompanion[ShapeItConfig] {
  
  object Keys {
    val workDirKey = "workDir"
    val executableKey = "executable"
    val scriptKey = "script"
    val vcfFileKey = "vcfFile"
    val mapFileKey = "mapFile"
    val sampleFileKey = "sampleFile"
    val hapFileKey = "hapFile"
    val logFileKey = "logFile"
    val numThreadsKey = "numThreads"
  }

  override def fromConfig(config: Config): Try[ShapeItConfig] = {
    import Keys._
    import PathEnrichments._
    import ConfigEnrichments._

    for {
      workDir <- config.tryGetPath(workDirKey)
      executable <- config.tryGetPath(executableKey)
      script <- config.tryGetPath(scriptKey)
      vcfFile <- workDir / config.tryGetString(vcfFileKey)
      mapFile <- workDir / config.tryGetString(mapFileKey)
      sampleFile <- workDir / config.tryGetString(sampleFileKey)
      hapFile <- workDir / config.tryGetString(hapFileKey)
      logFile <- workDir / config.tryGetString(logFileKey)
      numThreads <- config.tryGetInt(numThreadsKey)
    } yield {
      ShapeItConfig(
        workDir, 
        executable, 
        script,
        vcfFile,
        mapFile,
        sampleFile,
        hapFile,
        logFile,
        numThreads)
    }
  }
}