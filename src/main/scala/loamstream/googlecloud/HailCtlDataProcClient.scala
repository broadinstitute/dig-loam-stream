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

  override def stopCluster(): Unit = delegate.stopCluster()

  override def isClusterRunning: Boolean = delegate.isClusterRunning

  override def startCluster(clusterConfig: ClusterConfig): Unit = {
    debug(s"Starting cluster '${googleConfig.clusterId}'")

    import HailCtlDataProcClient.startClusterTokens
    
    val exitCode = runCommand(googleConfig, hailConfig, startClusterTokens(googleConfig, clusterConfig).mkString(" "))

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

  private[googlecloud] def startClusterTokens(
      googleConfig: GoogleCloudConfig, 
      clusterConfig: ClusterConfig): Seq[String] = {
    
    val tokens: Seq[String] = Seq(
      "--project",
      googleConfig.projectId,
      "--region",
      googleConfig.region,
      "--zone",
      clusterConfig.zone,
      "--master-machine-type",
      clusterConfig.masterMachineType,
      "--master-boot-disk-size",
      clusterConfig.masterBootDiskSize.toString,
      "--num-workers",
      clusterConfig.numWorkers.toString,
      "--worker-machine-type",
      clusterConfig.workerMachineType,
      "--worker-boot-disk-size",
      clusterConfig.workerBootDiskSize.toString,
      "--num-preemptible-workers",
      clusterConfig.numPreemptibleWorkers.toString,
      "--preemptible-worker-boot-disk-size",
      clusterConfig.preemptibleWorkerBootDiskSize.toString,
      "--num-worker-local-ssds",
      clusterConfig.numWorkerLocalSsds.toString,
      "--properties",
      clusterConfig.properties,
      "--max-idle",
      clusterConfig.maxClusterIdleTime,
      googleConfig.clusterId)
    
    hailctlTokens(googleConfig)("start")(tokens: _*)
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
                                 |CLOUDSDK_DATAPROC_REGION="${googleConfig.region}"
                                 |${command}""".stripMargin

    CloudSdkDataProcWrapper.runProcess(fullScriptContents, "hailctl")
  }
}
