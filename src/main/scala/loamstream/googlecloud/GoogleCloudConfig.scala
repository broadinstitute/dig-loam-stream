package loamstream.googlecloud

import java.nio.file.Path
import com.typesafe.config.Config
import scala.util.Try
import loamstream.util.ConfigEnrichments
import GoogleCloudConfig.Defaults

/**
 * @author clint
 * Nov 28, 2016
 */
final case class GoogleCloudConfig(
                                    gcloudBinary: Path,
                                    projectId: String,
                                    clusterId: String,
                                    credential: Path,
                                    zone: String = Defaults.zone,
                                    masterMachineType: String = Defaults.masterMachineType,
                                    masterBootDiskSize: Int = Defaults.masterBootDiskSize, // in GB
                                    numWorkers: Int = Defaults.numWorkers, // minimum 2
                                    workerMachineType: String = Defaults.workerMachineType,
                                    workerBootDiskSize: Int = Defaults.workerBootDiskSize, // in GB
                                    imageVersion: String = Defaults.imageVersion, // 2.x not supported by Hail
                                    scopes: String = Defaults.scopes)
    
object GoogleCloudConfig {
  object Defaults { // for creating a minimal cluster
    val zone: String = "us-central1-f"
    val masterMachineType: String = "n1-standard-1"
    val masterBootDiskSize: Int = 20
    val numWorkers: Int = 2
    val workerMachineType: String ="n1-standard-1"
    val workerBootDiskSize: Int = 20
    val imageVersion: String = "1.0"
    val scopes: String = "https://www.googleapis.com/auth/cloud-platform"
  }
  
  def fromConfig(config: Config): Try[GoogleCloudConfig] = {
    import ConfigEnrichments._
    
    def path(s: String): String = s"loamstream.googlecloud.$s"
    
    def tryGetPath(key: String): Try[Path] = config.tryGetPath(path(key))
    def tryGetString(key: String): Try[String] = config.tryGetString(path(key))
    def tryGetInt(key: String): Try[Int] = config.tryGetInt(path(key))
    
    def getStringOrElse(key: String, default: String): String = tryGetString(key).getOrElse(default)
    
    def getIntOrElse(key: String, default: Int): Int = tryGetInt(key).getOrElse(default)
    
    for {
      gcloudBinary <- tryGetPath("gcloudBinary")
      projectId <- tryGetString("projectId")
      clusterId <- tryGetString("clusterId")
      credential <- tryGetPath("credential")
      zone = getStringOrElse("zone", Defaults.zone)
      masterMachineType = getStringOrElse("masterMachineType", Defaults.masterMachineType)
      masterBootDiskSize = getIntOrElse("masterBootDiskSize", Defaults.masterBootDiskSize)
      numWorkers = getIntOrElse("numWorkers", Defaults.numWorkers)
      workerMachineType = getStringOrElse("workerMachineType", Defaults.workerMachineType)
      workerBootDiskSize = getIntOrElse("workerBootDiskSize", Defaults.workerBootDiskSize)
      imageVersion = getStringOrElse("imageVersion", Defaults.imageVersion)
      scopes = getStringOrElse("scopes", Defaults.scopes)
    } yield {
      GoogleCloudConfig(
        gcloudBinary,
        projectId,
        clusterId,
        credential,
        zone,
        masterMachineType,
        masterBootDiskSize,
        numWorkers,
        workerMachineType,
        workerBootDiskSize,
        imageVersion,
        scopes)
    }
  }
}