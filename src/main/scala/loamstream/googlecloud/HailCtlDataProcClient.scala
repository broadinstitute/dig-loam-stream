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
    for {
      wrapper <- CloudSdkDataProcWrapper.fromConfig(googleConfig)
    } yield {
      new HailCtlDataProcClient(googleConfig, hailConfig, new CloudSdkDataProcWrapper(googleConfig))
    }
  }

  private[googlecloud] def startClusterTokens(config: GoogleCloudConfig): Seq[String] = {
    val tokens: Seq[String] = Seq(
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

    CloudSdkDataProcWrapper.runProcess(fullScriptContents, "hailctl")
  }
}
