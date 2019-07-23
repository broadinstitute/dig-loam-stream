package loamstream.googlecloud

import loamstream.util.Loggable
import loamstream.util.Paths.normalize
import scala.util.Try
import scala.util.Success
import loamstream.util.Tries
import loamstream.util.ExitCodes
import scala.concurrent.duration.Duration
import loamstream.util.BashScript
import java.nio.file.Paths

/**
 * @author clint
 * Nov 28, 2016
 */
final class CloudSdkDataProcWrapper private[googlecloud] (
  config: GoogleCloudConfig,
  runCommand: Seq[String] => Int = CloudSdkDataProcWrapper.runCommand) extends 
      DataProcClient.CanStop with DataProcClient.CanTellIfRunning with Loggable {

  import CloudSdkDataProcWrapper.{
    deleteClusterTokens,
    isClusterRunningTokens
  }

  override def stopCluster(): Unit = {
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
}

object CloudSdkDataProcWrapper extends Loggable {
  def fromConfig(config: GoogleCloudConfig): Try[CloudSdkDataProcWrapper] = {
    val gcloudBinary = config.gcloudBinary.toFile

    if (gcloudBinary.exists && gcloudBinary.canExecute) {
      Success(new CloudSdkDataProcWrapper(config))
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
  
  private[googlecloud] def gcloudTokens(config: GoogleCloudConfig)(verb: String)(args: String*): Seq[String] = {
    val gcloud = normalize(config.gcloudBinary)

    gcloud +: "beta" +: "dataproc" +: "clusters" +: verb +: "--project" +: config.projectId +: args
  }

  private def runCommand(tokens: Seq[String]): Int = runProcess(tokens.mkString(" "), "Google Cloud SDK")
  
  private[googlecloud] def runProcess(commandLine: String, logPrefix: String): Int = {
    
    debug(s"Running ${logPrefix} command: '${commandLine}'")
    
    import scala.sys.process._
    
    // STDERR messages aren't logged as 'error' because 'gcloud' writes a lot of non-error messages to STDERR
    val processLogger = ProcessLogger(
      line => info(s"${logPrefix}: ${line}"),
      line => info(s"${logPrefix} (via stderr): ${line}"))

    val processBuilder = BashScript.fromCommandLineString(commandLine).processBuilder(Paths.get("."))
      
    val exitCode = processBuilder.!(processLogger)

    debug(s"Got status code $exitCode from running '${commandLine}'")

    exitCode
  }
}
