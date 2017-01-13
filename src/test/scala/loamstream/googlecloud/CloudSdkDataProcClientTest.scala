package loamstream.googlecloud

import java.nio.file.Paths

import loamstream.util.PathUtils
import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 29, 2016
 */
final class CloudSdkDataProcClientTest extends FunSuite {
  //scalastyle:off magic.number
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

  private val examplePath = PathUtils.newAbsolute("foo", "bar", "baz")
      
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
      "--image-version",
      config.imageVersion,
      "--scopes",
      config.scopes,
      "--project",
      config.projectId)
    
    assert(tokens === expected)
  }
  //scalastyle:on magic.number
}