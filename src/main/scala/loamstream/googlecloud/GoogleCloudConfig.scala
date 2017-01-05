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
    gcloudBinaryPath: Path, 
    projectId: String, //broadinstitute.com:cmi-gce-01
    clusterId: String, //"minimal"
    numWorkers: Int, //2
    zone: String = Defaults.zone,
    masterMachineType: String = Defaults.masterMachineType,
    masterBootDiskSize: Int = Defaults.masterBootDiskSize, //gigs?
    workerMachineType: String = Defaults.workerMachineType,
    workerBootDiskSize: Int = Defaults.workerBootDiskSize, //gigs?
    imageVersion: String = Defaults.imageVersion,
    scopes: String = Defaults.scopes)
    
object GoogleCloudConfig {
  object Defaults {
    val zone: String = "us-central1-f"
    val masterMachineType: String = "n1-standard-1"
    val masterBootDiskSize: Int = 20 //gigs?
    val workerMachineType: String ="n1-standard-1"
    val workerBootDiskSize: Int = 20 //gigs?
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
      gcloudBinaryPath <- tryGetPath("gcloudBinaryPath")
      projectId <- tryGetString("projectId")
      clusterId <- tryGetString("clusterId")
      numWorkers <- tryGetInt("numWorkers")
      zone = getStringOrElse("zone", Defaults.zone)
      masterMachineType = getStringOrElse("masterMachineType", Defaults.masterMachineType)
      masterBootDiskSize = getIntOrElse("masterBootDiskSize", Defaults.masterBootDiskSize)
      workerMachineType = getStringOrElse("workerMachineType", Defaults.workerMachineType)
      workerBootDiskSize = getIntOrElse("workerBootDiskSize", Defaults.workerBootDiskSize)
      imageVersion = getStringOrElse("imageVersion", Defaults.imageVersion)
      scopes = getStringOrElse("scopes", Defaults.scopes)
    } yield {
      GoogleCloudConfig(
        gcloudBinaryPath, 
        projectId,
        clusterId,
        numWorkers,
        zone,
        masterMachineType,
        masterBootDiskSize,
        workerMachineType,
        workerBootDiskSize,
        imageVersion,
        scopes)
    }
  }
}