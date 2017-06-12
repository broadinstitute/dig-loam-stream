package loamstream.googlecloud

import java.nio.file.Path

import scala.util.Try

import com.typesafe.config.Config
import loamstream.conf.ValueReaders

import GoogleCloudConfig.Defaults

/**
 * @author clint
 * Nov 28, 2016
 */
final case class GoogleCloudConfig(
    gcloudBinary: Path,
    projectId: String,
    clusterId: String,
    credentialsFile: Path,
    zone: String = Defaults.zone,
    masterMachineType: String = Defaults.masterMachineType,
    masterBootDiskSize: Int = Defaults.masterBootDiskSize, 
    numWorkers: Int = Defaults.numWorkers, 
    workerMachineType: String = Defaults.workerMachineType,
    workerBootDiskSize: Int = Defaults.workerBootDiskSize, 
    imageVersion: String = Defaults.imageVersion, 
    scopes: String = Defaults.scopes,
    properties: String = Defaults.properties,
    initializationActions: String = Defaults.initializationActions)
    
object GoogleCloudConfig {
  object Defaults { // for creating a minimal cluster
    val zone: String = "us-central1-f"
    val masterMachineType: String = "n1-standard-1"
    val masterBootDiskSize: Int = 20 // in GB
    val numWorkers: Int = 2
    val workerMachineType: String = "n1-standard-1"
    val workerBootDiskSize: Int = 20 // in GB
    val imageVersion: String = "1.0" // 2.x not supported by Hail
    val scopes: String = "https://www.googleapis.com/auth/cloud-platform"
    val properties: String = {
      "spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M," +
      "spark:spark.driver.memory=45g,spark:spark.driver.maxResultSize=30g,spark:spark.task.maxFailures=20," +
      "spark:spark.kryoserializer.buffer.max=1g,hdfs:dfs.replication=1"
    }
    val initializationActions: String = "gs://loamstream/hail/hail-init.sh"
  }
  
  def fromConfig(config: Config): Try[GoogleCloudConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    
    //NB: Ficus now marshals the contents of loamstream.googlecloud into a GoogleCloudConfig instance.
    //Names of fields in GoogleCloudConfig and keys under loamstream.googlecloud must match.
    Try(config.as[GoogleCloudConfig]("loamstream.googlecloud"))
  }
}
