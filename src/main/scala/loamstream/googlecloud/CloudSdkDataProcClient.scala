package loamstream.googlecloud

import loamstream.util.Loggable
import loamstream.util.PathUtils.normalize
import scala.util.Try
import scala.util.Success
import loamstream.util.Tries

/**
 * @author clint
 * Nov 28, 2016
 */
final class CloudSdkDataProcClient private[googlecloud] (config: GoogleCloudConfig) extends 
    DataProcClient with Loggable {
  
  import CloudSdkDataProcClient._
  
  override def deleteCluster(): Unit = {
    debug(s"Deleting cluster '${config.clusterId}'")
    
    runCommand(deleteClusterTokens(config))
  }
  
  override def isClusterRunning: Boolean = {
    val result = runCommand(isClusterRunningTokens(config)) == 0
    
    debug(s"Cluster '${config.clusterId}' running? $result")
    
    result
  }
  
  override def startCluster(): Unit = {
    debug(s"Starting cluster '${config.clusterId}'")
    
    runCommand(startClusterTokens(config))
  }
}

object CloudSdkDataProcClient extends Loggable {
  def fromConfig(config: GoogleCloudConfig): Try[CloudSdkDataProcClient] = {
    val gcloudBinary = config.gcloudBinaryPath.toFile
    
    if(gcloudBinary.exists && gcloudBinary.canExecute) {
      Success(new CloudSdkDataProcClient(config))
    } else {
      Tries.failure(s"gcloud executable not found at ${config.gcloudBinaryPath} or not executable")
    }
  }
  
  private[googlecloud] def deleteClusterTokens(config: GoogleCloudConfig): Seq[String] = {
    gcloudTokens(config)("delete", config.clusterId)
  }
  
  private[googlecloud] def isClusterRunningTokens(config: GoogleCloudConfig): Seq[String] = {
    gcloudTokens(config)("describe", config.clusterId)
  }
  
  private[googlecloud] def startClusterTokens(config: GoogleCloudConfig): Seq[String] = {
    gcloudTokens(config)(
        "create", 
        config.clusterId,
        "--zone",
        config.zone,
        "--master-machine-type",
        config.masterMachineType,
        "--master-boot-disk-size",
        config.masterBootDiskSize.toString,
        "--num-workers",
        config.numWorkers.toString,
        "--worker-machine-type",
        config.workerMachineType,
        "--worker-boot-disk-size",
        config.workerBootDiskSize.toString,
        "--image-version",
        config.imageVersion,
        "--scopes",
        config.scopes,
        "--project",
        config.projectId)
  }
  
  private[googlecloud] def gcloudTokens(config: GoogleCloudConfig)(args: String*): Seq[String] = {
    val gcloud = normalize(config.gcloudBinaryPath)
    
    gcloud +: "dataproc" +: "clusters" +: args
  }
  
  private def runCommand(tokens: Seq[String]): Int = {
    val commandStringApproximation = tokens.mkString(" ")
    
    debug(s"Running Google Cloud SDK command: '$commandStringApproximation'")
    
    import scala.sys.process._
    
    val processLogger = ProcessLogger(line => info(s"STDOUT: $line"), line => error(s"STDERR: $line"))
    
    val result = Process(tokens).!(processLogger)
    
    debug(s"Got status code $result from running '$commandStringApproximation'")

    result
  }
}
