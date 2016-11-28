package loamstream.googlecloud

import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.Loggable

/**
 * @author clint
 * Nov 28, 2016
 */
final class CloudSdkDataProcClient(config: GoogleCloudConfig) extends DataProcClient with Loggable {
  override def deleteCluster(): Unit = {
    debug(s"Deleting cluster '${config.clusterId}'")
    
    runClusterCommand("delete", config.clusterId)
  }
  
  override def isClusterRunning: Boolean = {
    val result = runClusterCommand("describe", config.clusterId) == 0
    
    debug(s"Cluster '${config.clusterId}' running? $result")
    
    result
  }
  
  override def startCluster(): Unit = {
    debug(s"Starting cluster '${config.clusterId}'")
    
    runClusterCommand(
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
  
  private def runClusterCommand(args: String*): Int = runCloudCommand("clusters" +: args)
  
  private def runCloudCommand(args: Seq[String]): Int = {
    import scala.sys.process._
    
    val tokens = Seq("gcloud", "dataproc") ++ args
    
    val commandStringApproximation = tokens.mkString(" ")
    
    debug(s"Running Google Clound SDK command: '$commandStringApproximation'")
    
    val result = Process(tokens).!(CommandLineJob.stdErrProcessLogger)
    
    debug(s"Got status code $result from running '$commandStringApproximation'")

    result
  }
}