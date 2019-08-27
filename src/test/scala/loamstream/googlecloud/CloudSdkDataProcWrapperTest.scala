package loamstream.googlecloud

import org.scalatest.FunSuite

import loamstream.util.BashScript.Implicits.BashPath
import loamstream.util.ExitCodeException
import loamstream.util.ExitCodes
import loamstream.util.Paths

/**
 * @author clint
 * Nov 29, 2016
 */
final class CloudSdkDataProcWrapperTest extends FunSuite {

  import loamstream.TestHelpers.path

  private val config = GoogleCloudConfig(
    gcloudBinary = path("/foo/bar/baz"),
    gsutilBinary = path("/blah/blah/blah"),
    projectId = "pid",
    clusterId = "cid",
    credentialsFile = path("N/A"),
    //non-default values
    defaultClusterConfig = ClusterConfig(
      numWorkers = 42,
      zone = "z",
      masterMachineType = "mmt",
      masterBootDiskSize = 99,
      workerMachineType = "wmt",
      workerBootDiskSize = 17,
      numPreemptibleWorkers = 123,
      preemptibleWorkerBootDiskSize = 11,
      properties = "p,r,o,p,s",
      maxClusterIdleTime = "42m"))
    
  private val examplePath = Paths.newAbsolute("foo", "bar", "baz")

  private def alwaysFails(tokens: Seq[String]): Int = 42

  private def alwaysSucceeds(tokens: Seq[String]): Int = ExitCodes.success

  test("deleteCluster") {
    val client = new CloudSdkDataProcWrapper(config, alwaysSucceeds)

    //Shouldn't throw
    client.stopCluster()
  }

  test("deleteCluster - should throw") {
    val client = new CloudSdkDataProcWrapper(config, alwaysFails)

    intercept[ExitCodeException] {
      client.stopCluster()
    }
  }

  test("isClusterRunning") {
    val client = new CloudSdkDataProcWrapper(config, alwaysSucceeds)

    assert(client.isClusterRunning)
  }

  test("isClusterRunning - not running") {
    val client = new CloudSdkDataProcWrapper(config, alwaysFails)

    assert(client.isClusterRunning === false)
  }

  test("gcloudTokens") {
    import CloudSdkDataProcWrapper.gcloudTokens

    val tokens = gcloudTokens(config)("foo")("--bar", "Baz")

    val expectedTokens = {
      Seq(examplePath.render, "beta", "dataproc", "clusters", "foo", "--project", config.projectId, "--bar", "Baz")
    }
    
    assert(tokens === expectedTokens)
  }

  test("isClusterRunningTokens") {
    import CloudSdkDataProcWrapper.isClusterRunningTokens

    val tokens = isClusterRunningTokens(config)

    val expectedTokens = Seq(
        examplePath.render, 
        "beta", 
        "dataproc", 
        "clusters", 
        "describe", 
        "--project", 
        config.projectId, 
        config.clusterId)
    
    assert(tokens === expectedTokens)
  }

  test("deleteClusterTokens") {
    import CloudSdkDataProcWrapper.deleteClusterTokens

    val tokens = deleteClusterTokens(config)

    val expectedTokens = Seq(
        examplePath.render, 
        "beta", 
        "dataproc", 
        "clusters", 
        "delete", 
        "--project", 
        config.projectId, 
        config.clusterId) 
    
    assert(tokens === expectedTokens)
  }
}
