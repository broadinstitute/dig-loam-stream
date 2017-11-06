package loamstream.googlecloud

import loamstream.util.PathUtils
import org.scalatest.FunSuite
import loamstream.util.ExitCodes
import loamstream.util.ExitCodeException
import loamstream.TestHelpers

/**
 * @author clint
 * Nov 29, 2016
 */
final class CloudSdkDataProcClientTest extends FunSuite {
  
  import TestHelpers.path
  
  private val config = GoogleCloudConfig(
      gcloudBinary = path("/foo/bar/baz"),
      gsutilBinary = path("/blah/blah/blah"),
      projectId = "pid",
      clusterId = "cid",
      credentialsFile = path("N/A"),
      numWorkers = 42,
      //non-default values
      zone = "z",
      masterMachineType = "mmt",
      masterBootDiskSize = 99,
      workerMachineType = "wmt",
      workerBootDiskSize = 17,
      numPreemptibleWorkers = 123,
      preemptibleWorkerBootDiskSize = 11,
      imageVersion = "42.2.1.0",
      scopes = "ses",
      properties = "p,r,o,p,s",
      initializationActions = "gs://example.com/blah/foo.sh")
  
  private val examplePath = PathUtils.newAbsolute("foo", "bar", "baz")
      
  private def alwaysFails(tokens: Seq[String]): Int = 42
  
  private def alwaysSucceeds(tokens: Seq[String]): Int = ExitCodes.success
  
  test("startCluster") {
    val client = new CloudSdkDataProcClient(config, alwaysSucceeds)
    
    //Shouldn't throw
    client.startCluster()
  }
  
  test("startCluster - should throw") {
    val client = new CloudSdkDataProcClient(config, alwaysFails)
    
    intercept[ExitCodeException] {
      client.startCluster()
    }
  }
  
  test("deleteCluster") {
    val client = new CloudSdkDataProcClient(config, alwaysSucceeds)
    
    //Shouldn't throw
    client.deleteCluster()
  }
  
  test("deleteCluster - should throw") {
    val client = new CloudSdkDataProcClient(config, alwaysFails)
    
    intercept[ExitCodeException] {
      client.deleteCluster()
    }
  }
  
  test("isClusterRunning") {
    val client = new CloudSdkDataProcClient(config, alwaysSucceeds)
    
    assert(client.isClusterRunning)
  }
  
  test("isClusterRunning - not running") {
    val client = new CloudSdkDataProcClient(config, alwaysFails)
    
    assert(client.isClusterRunning === false)
  }
  
  test("gcloudTokens") {
    import CloudSdkDataProcClient.gcloudTokens
    
    val tokens = gcloudTokens(config)("foo", "--bar", "Baz")
    
    assert(tokens === Seq(examplePath.toString, "dataproc", "clusters", "foo", "--bar", "Baz"))
  }
  
  test("isClusterRunningTokens") {
    import CloudSdkDataProcClient.isClusterRunningTokens
    
    val tokens = isClusterRunningTokens(config)
    
    assert(tokens === Seq(examplePath.toString, "dataproc", "clusters", "describe", config.clusterId))
  }
  
  test("deleteClusterTokens") {
    import CloudSdkDataProcClient.deleteClusterTokens
    
    val tokens = deleteClusterTokens(config)
    
    assert(tokens === Seq(examplePath.toString, "dataproc", "clusters", "delete", config.clusterId))
  }
  
  test("startClusterTokens") {
    import CloudSdkDataProcClient.startClusterTokens
    
    val tokens = startClusterTokens(config)
    
    val expected = Seq(
      examplePath.toString,
      "dataproc",
      "clusters",
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
      "--num-preemptible-workers",
      config.numPreemptibleWorkers.toString,
      "--preemptible-worker-boot-disk-size",
      config.preemptibleWorkerBootDiskSize.toString,
      "--image-version",
      config.imageVersion,
      "--scopes",
      config.scopes,
      "--project",
      config.projectId,
      "--properties",
      config.properties,
      "--initialization-actions",
      config.initializationActions)
    
    assert(tokens === expected)
  }
  //scalastyle:on magic.number
}
