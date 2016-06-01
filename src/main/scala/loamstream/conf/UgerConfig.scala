package loamstream.conf

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}
import java.nio.file.Path
import scala.util.Try
import loamstream.util.ConfigEnrichments
import java.io.FileNotFoundException
import scala.util.control.NonFatal
import scala.util.Failure
import scala.util.Success

/**
 * Created on: 5/4/16 
 * @author Kaan Yuksel 
 */
case class UgerConfig private (ugerWorkDir: Path, ugerLogFile: Path)

object UgerConfig {

  object Keys extends TypesafeConfig.KeyHolder("uger") {
    val ugerWorkDirKey = key("workDir")
    val ugerLogFileKey = key("logFile")
  }

  def apply(configFile: String): Try[UgerConfig] = {
    def tryFile(fileName: String): Try[Path] = {
      val path = Paths.get(fileName)
      
      if(path.toFile.exists) { Success(path) }
      else { Failure(new FileNotFoundException(s"Can't find '$fileName'")) }
    }
    
    for {
      file <- tryFile(configFile)
      config = TypesafeConfig.fromFile(file)
      result <- UgerConfig(config)
    } yield result
  }

  def apply(config: Config): Try[UgerConfig] = {
    val ugerProps = TypesafeConfigLproperties(config)

    import Keys._
    
    for {
      ugerWorkDir <- ugerProps.tryGetPath(ugerWorkDirKey)
      ugerLogFile <- ugerProps.tryGetPath(ugerLogFileKey)
    } yield {
      UgerConfig(ugerWorkDir, ugerLogFile)
    }
  }
}
