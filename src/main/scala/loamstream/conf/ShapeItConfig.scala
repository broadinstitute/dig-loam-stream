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
    mapFile: Path, 
    logFile: Path, 
    numThreads: Int)
    
object ShapeItConfig extends ConfigCompanion[ShapeItConfig] {
  
  object Keys {
    val workDirKey = "workDir"
    val executableKey = "executable"
    val scriptKey = "script"
    val mapFileKey = "mapFile"
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
      mapFile <- workDir / config.tryGetString(mapFileKey)
      logFile <- workDir / config.tryGetString(logFileKey)
      numThreads <- config.tryGetInt(numThreadsKey)
    } yield {
      ShapeItConfig(
        workDir, 
        executable, 
        script, 
        mapFile, 
        logFile, 
        numThreads)
    }
  }
}