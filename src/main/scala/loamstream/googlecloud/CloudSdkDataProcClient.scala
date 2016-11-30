package loamstream.googlecloud

import loamstream.model.jobs.commandline.CommandLineJob
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
  
  override def deleteCluster(): Unit = {
    debug(s"Deleting cluster '${config.clusterId}'")
    
    runCommand(deleteClusterTokens)
  }
  
  override def isClusterRunning: Boolean = {
    val result = runCommand(isClusterRunningTokens) == 0
    
    debug(s"Cluster '${config.clusterId}' running? $result")
    
    result
  }
  
  override def startCluster(): Unit = {
    debug(s"Starting cluster '${config.clusterId}'")
    
    runCommand(startClusterTokens)
  }

  private[googlecloud] def deleteClusterTokens: Seq[String] = gcloudTokens("delete", config.clusterId)
  
  private[googlecloud] def isClusterRunningTokens: Seq[String] = gcloudTokens("describe", config.clusterId)
  
  private[googlecloud] def startClusterTokens: Seq[String] = {
    gcloudTokens(
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
  
  private[googlecloud] def gcloudTokens(args: String*): Seq[String] = {
    val gcloud = normalize(config.gcloudBinaryPath)
    
    gcloud +: "dataproc" +: "clusters" +: args
  }
  
  private def runCommand(tokens: Seq[String]): Int = {
    val commandStringApproximation = tokens.mkString(" ")
    
    debug(s"Running Google Clound SDK command: '$commandStringApproximation'")
    
    import scala.sys.process._
    
    val result = Process(tokens).!(CommandLineJob.stdErrProcessLogger)
    
    debug(s"Got status code $result from running '$commandStringApproximation'")

    result
  }
}

object CloudSdkDataProcClient {
  def fromConfig(config: GoogleCloudConfig): Try[CloudSdkDataProcClient] = {
    val gcloudBinary = config.gcloudBinaryPath.toFile
    
    if(gcloudBinary.exists && gcloudBinary.canExecute) {
      Success(new CloudSdkDataProcClient(config))
    } else {
      Tries.failure(s"gcloud executable not found at ${config.gcloudBinaryPath} or not executable")
    }
  }
}