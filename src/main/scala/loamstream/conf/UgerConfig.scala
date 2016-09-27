package loamstream.conf

import java.nio.file.Path

import com.typesafe.config.Config

import scala.util.Try

/**
  * Created on: 5/4/16
  *
  * @author Kaan Yuksel
  */
final case class UgerConfig(ugerWorkDir: Path, ugerLogFile: Path, ugerMaxNumJobs: Int)

object UgerConfig extends ConfigCompanion[UgerConfig] {

  object Keys extends TypesafeConfig.KeyHolder("uger") {
    val ugerWorkDirKey = key("workDir")
    val ugerLogFileKey = key("logFile")
    val ugerMaxNumJobsKey = key("maxNumJobs")
  }

  override def fromConfig(config: Config): Try[UgerConfig] = {
    val ugerProps = TypesafeConfigLproperties(config)

    import Keys._

    for {
      ugerWorkDir <- ugerProps.tryGetPath(ugerWorkDirKey)
      ugerLogFile <- ugerProps.tryGetPath(ugerLogFileKey)
      ugerMaxNumJobs <- ugerProps.tryGetInt(ugerMaxNumJobsKey)
    } yield {
      UgerConfig(ugerWorkDir, ugerLogFile, ugerMaxNumJobs)
    }
  }
}
