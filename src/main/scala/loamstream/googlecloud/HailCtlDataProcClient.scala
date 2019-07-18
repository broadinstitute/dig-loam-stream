package loamstream.googlecloud

import java.nio.file.Paths

import scala.util.Success
import scala.util.Try

import HailCtlDataProcClient.DelegateFn
import loamstream.util.BashScript
import loamstream.util.ExitCodes
import loamstream.util.Loggable
import loamstream.util.Tries

/**
 * @author clint
 * Nov 28, 2016
 */
final class HailCtlDataProcClient private[googlecloud] (
  googleConfig: GoogleCloudConfig,
  hailConfig: HailConfig,
  delegate: DataProcClient.CanStopAndTellIfRunning,
  runCommand: DelegateFn = HailCtlDataProcClient.runCommand) extends DataProcClient with Loggable {

  override def deleteCluster(): Unit = delegate.deleteCluster()

  override def isClusterRunning: Boolean = delegate.isClusterRunning

  override def startCluster(): Unit = {
    debug(s"Starting cluster '${googleConfig.clusterId}'")

    import HailCtlDataProcClient.startClusterTokens
    
    val exitCode = runCommand(googleConfig, hailConfig, startClusterTokens(googleConfig).mkString(" "))

    ExitCodes.throwIfFailure(exitCode)
  }
}

object HailCtlDataProcClient extends Loggable {
  type DelegateFn = (GoogleCloudConfig, HailConfig, String) => Int
  
  def fromConfigs(googleConfig: GoogleCloudConfig, hailConfig: HailConfig): Try[HailCtlDataProcClient] = {
    val gcloudBinary = googleConfig.gcloudBinary.toFile

    if (gcloudBinary.exists && gcloudBinary.canExecute) {
      Success(new HailCtlDataProcClient(googleConfig, hailConfig, new CloudSdkDataProcClient(googleConfig)))
    } else {
      Tries.failure(s"gcloud executable not found at ${googleConfig.gcloudBinary} or not executable")
    }
  }

  private[googlecloud] def startClusterTokens(config: GoogleCloudConfig): Seq[String] = {
    val firstTokens: Seq[String] = Seq(
      "--project",
      config.projectId,
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
      "--properties",
      config.properties,
      "--max-idle",
      config.maxClusterIdleTime,
      config.clusterId)
    
    val metadataPart: Seq[String] = config.metadata.map(_.trim).filter(_.nonEmpty) match {
      case Some(md) => Seq("--metadata", md)
      case None => Nil
    }
    
    val tokens = firstTokens ++ metadataPart

    hailctlTokens(config)("start")(tokens: _*)
  }
  
  private[googlecloud] def hailctlTokens(config: GoogleCloudConfig)(verb: String)(args: String*): Seq[String] = {
    "hailctl" +: "dataproc" +: verb +: args
  }

  private def runCommand(googleConfig: GoogleCloudConfig, hailConfig: HailConfig, command: String): Int = {
    import loamstream.util.Paths.normalize
    
    val fullScriptContents = s"""|source ~/.bashrc
                                 |conda activate ${hailConfig.condaEnv}
                                 |PATH="${normalize(googleConfig.gcloudBinary.getParent)}":$${PATH}
                                 |CLOUDSDK_CORE_PROJECT="${googleConfig.projectId}"
                                 |${command}""".stripMargin

    debug(s"Running Google Cloud SDK command: '$fullScriptContents'")

    val bashScript = BashScript.fromCommandLineString(fullScriptContents)
    
    import scala.sys.process._

    // STDERR messages aren't logged as 'error' because gcloud/hailctl writes a lot of non-error messages to STDERR
    val processLogger = ProcessLogger(
      line => info(s"hailctl: $line"),
      line => info(s"hailctl (via stderr): $line"))

    val result = bashScript.processBuilder(Paths.get(".")).!(processLogger)

    debug(s"Got status code $result from running '$fullScriptContents'")

    result
  }
}
