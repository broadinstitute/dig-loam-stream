package loamstream.googlecloud

import org.scalatest.FunSuite
import loamstream.TestHelpers

/**
 * @author clint
 * Jul 22, 2019
 */
final class HailCtlDataProcClientTest extends FunSuite {
  private def canStop[A](f: => A): DataProcClient.CanStop = new DataProcClient.CanStop {
    override def stopCluster(): Unit = f
  }
  
  private def canTellIfRunning(f: => Boolean): DataProcClient.CanTellIfRunning = new DataProcClient.CanTellIfRunning {
    override def isClusterRunning: Boolean = f
  }
  
  test("deleteCluster / isClusterRunning") {
    var canStopInvoked = 0
    var isClusterRunningInvoked = 0
    
    val delegate: DataProcClient = new DataProcClient {
      override def stopCluster(): Unit =  {
        canStopInvoked += 1
      }
      
      override def isClusterRunning: Boolean = {
        isClusterRunningInvoked += 1
      
        true
      }
      
      override def startCluster(clusterConfig: ClusterConfig): Unit = ???
    }
    
    val hailCtlClient = new HailCtlDataProcClient(TestHelpers.config.googleConfig.get, HailConfig("foo-env"), delegate)
    
    assert(canStopInvoked === 0)
    assert(isClusterRunningInvoked === 0)
    
    assert(hailCtlClient.isClusterRunning === true)
    assert(canStopInvoked === 0)
    assert(isClusterRunningInvoked === 1)
    
    hailCtlClient.stopCluster()
    assert(canStopInvoked === 1)
    assert(isClusterRunningInvoked === 1)
  }
  
  test("startClusterTokens") {
    import HailCtlDataProcClient.startClusterTokens
    import TestHelpers.path
    
    val clusterConfig = ClusterConfig(
        zone = "some-zone",
        masterMachineType = "some-mmt",
        masterBootDiskSize = 42,
        numWorkers = 99,
        workerMachineType = "some-wmt",
        workerBootDiskSize = 123,
        numPreemptibleWorkers = 17,
        preemptibleWorkerBootDiskSize = 11,
        numWorkerLocalSsds = 1,
        properties = "some-properties",
        maxClusterIdleTime = "13m")
    
    val googleConfig = GoogleCloudConfig(
        gcloudBinary = path("/foo/bar/gcloud"),
        gsutilBinary = path("/blah/blah/gsutil"),
        projectId = "some-project-id",
        clusterId = "some-cluster-id",
        credentialsFile = path("some/creds/file"),
        region = "some-region",
        defaultClusterConfig = clusterConfig)
    
    val expected = Seq(
      "hailctl",
      "dataproc",
      "start",
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
      
    assert(startClusterTokens(googleConfig, clusterConfig) === expected)
  }
}
