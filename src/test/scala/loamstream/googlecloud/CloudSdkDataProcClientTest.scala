package loamstream.googlecloud

import scala.concurrent.duration._

import org.scalatest.FunSuite

import loamstream.util.BashScript.Implicits.BashPath
import loamstream.util.ExitCodeException
import loamstream.util.ExitCodes
import loamstream.util.Paths

/**
 * @author clint
 * Nov 29, 2016
 */
final class CloudSdkDataProcClientTest extends FunSuite {

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
      imageVersion = "42.2.1.0",
      scopes = "ses",
      properties = "p,r,o,p,s",
      initializationActions = "gs://example.com/blah/foo.sh",
      metadata = None,
      maxClusterIdleTime = "42m"))
    
  private val configWithMetadata = {
    config.copy(defaultClusterConfig = config.defaultClusterConfig.copy(metadata = Some("key=value")))
  }

  private val examplePath = Paths.newAbsolute("foo", "bar", "baz")

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

    val tokens = gcloudTokens(config)("foo")("--bar", "Baz")

    val expectedTokens = {
      Seq(examplePath.render, "beta", "dataproc", "clusters", "foo", "--project", config.projectId, "--bar", "Baz")
    }
    
    assert(tokens === expectedTokens)
  }

  test("isClusterRunningTokens") {
    import CloudSdkDataProcClient.isClusterRunningTokens

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
    import CloudSdkDataProcClient.deleteClusterTokens

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

  test("startClusterTokens - with metadata") {
    import CloudSdkDataProcClient.startClusterTokens

    val tokens = startClusterTokens(configWithMetadata)

    val expected = baseStartClusterTokens ++ Seq("--metadata", configWithMetadata.defaultClusterConfig.metadata.get)

    assert(tokens === expected)
  }
  
  test("startClusterTokens - no metadata") {
    import CloudSdkDataProcClient.startClusterTokens

    val tokens = startClusterTokens(config)

    val expected = baseStartClusterTokens

    assert(tokens === expected)
  }
  
  private val baseStartClusterTokens: Seq[String] = Seq(
      examplePath.render,
      "beta",
      "dataproc",
      "clusters",
      "create",
      "--project",
      config.projectId,
      config.clusterId,
      "--zone",
      config.defaultClusterConfig.zone,
      "--master-machine-type",
      config.defaultClusterConfig.masterMachineType,
      "--master-boot-disk-size",
      config.defaultClusterConfig.masterBootDiskSize.toString,
      "--num-workers",
      config.defaultClusterConfig.numWorkers.toString,
      "--worker-machine-type",
      config.defaultClusterConfig.workerMachineType,
      "--worker-boot-disk-size",
      config.defaultClusterConfig.workerBootDiskSize.toString,
      "--num-preemptible-workers",
      config.defaultClusterConfig.numPreemptibleWorkers.toString,
      "--preemptible-worker-boot-disk-size",
      config.defaultClusterConfig.preemptibleWorkerBootDiskSize.toString,
      "--image-version",
      config.defaultClusterConfig.imageVersion,
      "--scopes",
      config.defaultClusterConfig.scopes,
      "--properties",
      config.defaultClusterConfig.properties,
      "--initialization-actions",
      config.defaultClusterConfig.initializationActions,
      "--max-idle",
      "42m")
}
