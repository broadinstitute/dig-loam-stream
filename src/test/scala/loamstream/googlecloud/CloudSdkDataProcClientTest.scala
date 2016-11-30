package loamstream.googlecloud

import org.scalatest.FunSuite
import java.nio.file.Paths

/**
 * @author clint
 * Nov 29, 2016
 */
final class CloudSdkDataProcClientTest extends FunSuite {
  private val config = GoogleCloudConfig(
      gcloudBinaryPath = Paths.get("/foo/bar/baz"), 
      projectId = "pid",
      clusterId = "cid", 
      numWorkers = 42,
      //non-default values
      zone = "z",
      masterMachineType = "mmt",
      masterBootDiskSize = 99,
      workerMachineType = "wmt",
      workerBootDiskSize = 17,
      imageVersion = "42.2.1.0",
      scopes = "ses")
  
  private val client = new CloudSdkDataProcClient(config)
      
  test("gcloudTokens") {
    val tokens = client.gcloudTokens("foo", "--bar", "Baz")
    
    assert(tokens === Seq("/foo/bar/baz", "dataproc", "clusters", "foo", "--bar", "Baz"))
  }
  
  test("isClusterRunningTokens") {
    val tokens = client.isClusterRunningTokens
    
    assert(tokens === Seq("/foo/bar/baz", "dataproc", "clusters", "describe", config.clusterId))
  }
  
  test("deleteClusterTokens") {
    val tokens = client.deleteClusterTokens
    
    assert(tokens === Seq("/foo/bar/baz", "dataproc", "clusters", "delete", config.clusterId))
  }
  
  test("startClusterTokens") {
    val tokens = client.startClusterTokens
    
    val expected = Seq(
        "/foo/bar/baz", 
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
        "--image-version",
        config.imageVersion,
        "--scopes",
        config.scopes,
        "--project",
        config.projectId)
    
    assert(tokens === expected)
  }
}