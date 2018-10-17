package loamstream.googlecloud

import loamstream.util.Loggable
import loamstream.util.Paths.normalize
import scala.util.Try
import scala.util.Success
import loamstream.util.Tries
import loamstream.util.ExitCodes

/**
 * @author clint
 * Nov 28, 2016
 */
final class CloudSdkDataProcClient private[googlecloud] (
  config: GoogleCloudConfig,
  runCommand: Seq[String] => Int = CloudSdkDataProcClient.runCommand) extends DataProcClient with Loggable {

  import CloudSdkDataProcClient.{
    startClusterTokens,
    deleteClusterTokens,
    isClusterRunningTokens
  }

  override def deleteCluster(): Unit = {
    debug(s"Deleting cluster '${config.clusterId}'")

    val exitCode = runCommand(deleteClusterTokens(config))

    ExitCodes.throwIfFailure(exitCode)
  }

  override def isClusterRunning: Boolean = {
    val exitCode = runCommand(isClusterRunningTokens(config))

    val result = ExitCodes.isSuccess(exitCode)

    debug(s"Cluster '${config.clusterId}' running? $result")

    result
  }

  override def startCluster(): Unit = {
    debug(s"Starting cluster '${config.clusterId}'")

    val exitCode = runCommand(startClusterTokens(config))

    ExitCodes.throwIfFailure(exitCode)
  }
}

object CloudSdkDataProcClient extends Loggable {
  def fromConfig(config: GoogleCloudConfig): Try[CloudSdkDataProcClient] = {
    val gcloudBinary = config.gcloudBinary.toFile

    if (gcloudBinary.exists && gcloudBinary.canExecute) {
      Success(new CloudSdkDataProcClient(config))
    } else {
      Tries.failure(s"gcloud executable not found at ${config.gcloudBinary} or not executable")
    }
  }

  private[googlecloud] def deleteClusterTokens(config: GoogleCloudConfig): Seq[String] = {
    gcloudTokens(config)("delete")(config.clusterId)
  }

  private[googlecloud] def isClusterRunningTokens(config: GoogleCloudConfig): Seq[String] = {
    gcloudTokens(config)("describe")(config.clusterId)
  }

  private[googlecloud] def startClusterTokens(config: GoogleCloudConfig): Seq[String] = {
    val firstTokens: Seq[String] = Seq(
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
      "--num-preemptible-workers",
      config.numPreemptibleWorkers.toString,
      "--preemptible-worker-boot-disk-size",
      config.preemptibleWorkerBootDiskSize.toString,
      "--image-version",
      config.imageVersion,
      "--scopes",
      config.scopes,
      "--properties",
      config.properties,
      "--initialization-actions",
      config.initializationActions)
    
    val metadataPart: Seq[String] = config.metadata match {
      case Some(md) => Seq("--metadata", md)
      case None => Nil
    }
    
    val tokens = firstTokens ++ metadataPart

    gcloudTokens(config)("create")(tokens: _*)
  }

  private[googlecloud] def gcloudTokens(config: GoogleCloudConfig)(verb: String)(args: String*): Seq[String] = {
    val gcloud = normalize(config.gcloudBinary)

    gcloud +: "dataproc" +: "clusters" +: verb +: "--project" +: config.projectId +: args
  }

  private def runCommand(tokens: Seq[String]): Int = {
    val commandStringApproximation = tokens.mkString(" ")

    debug(s"Running Google Cloud SDK command: '$commandStringApproximation'")

    import scala.sys.process._

    // STDERR messages aren't logged as 'error' because 'gcloud' writes a lot of non-error messages to STDERR
    val processLogger = ProcessLogger(
      line => info(s"Google Cloud SDK: $line"),
      line => info(s"Google Cloud SDK (via stderr): $line"))

    val result = Process(tokens).!(processLogger)

    debug(s"Got status code $result from running '$commandStringApproximation'")

    result
  }
}
