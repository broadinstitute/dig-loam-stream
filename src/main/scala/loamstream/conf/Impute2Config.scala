package loamstream.conf

import java.nio.file.Path
import com.typesafe.config.Config
import scala.util.Try
import loamstream.util.PathEnrichments
import loamstream.util.ConfigEnrichments

/**
 * @author clint
 * @author kaan
 * date: Jun 13, 2016
 */
final case class Impute2Config(
    workDir: Path, 
    executable: Path)
    
object Impute2Config extends ConfigCompanion[Impute2Config] {
  
  object Keys {
    val workDirKey = "workDir"
    val executableKey = "executable"
  }

  override def fromConfig(config: Config): Try[Impute2Config] = {
    import Keys._
    import PathEnrichments._
    import ConfigEnrichments._
    
    for {
      workDir <- config.tryGetPath(workDirKey)
      executable <- config.tryGetPath(executableKey)
    } yield {
      Impute2Config(
        workDir, 
        executable)
    }
  }
}