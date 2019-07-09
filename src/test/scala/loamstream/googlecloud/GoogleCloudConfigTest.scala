package loamstream.googlecloud

import java.nio.file.Paths

import scala.concurrent.duration._
import scala.util.Try

import org.scalatest.FunSuite

import com.typesafe.config.ConfigFactory
import scala.util.Success

/**
 * @author clint
 * Nov 29, 2016
 */
final class GoogleCloudConfigTest extends FunSuite {
  private val gcloudBinaryPath = "/path/to/gcloud"
  private val gsutilBinaryPath = "/path/to/gsutil"
  private val projectId = "pid"
  private val clusterId = "cid"
  private val credentialsFile = "/path/to/credentialsFile"
  private val numWorkers = 42
  private val numPreemptibleWorkers = 123
  private val preemptibleWorkerBootDiskSize = 19
  private val zone = "z"
  private val masterMachineType = "mmt"
  private val masterBootDiskSize = 99
  private val workerMachineType = "wmt"
  private val workerBootDiskSize = 17
  private val imageVersion = "iv"
  private val scopes = "ses"
  private val properties = "p,r,o,p,s"
  private val initializationActions = "gs://example.com/foo.sh"
  private val metadata = "key=value"
  private val maxClusterIdleTime = "42h"

  test("fromConfig - defaults used") {
    val confString = s"""loamstream {
        googlecloud {
          gcloudBinary = "$gcloudBinaryPath"
          gsutilBinary = "$gsutilBinaryPath"
          projectId = "$projectId"
          clusterId = "$clusterId"
          credentialsFile = "$credentialsFile"
        }
      }"""

    val config = ConfigFactory.parseString(confString)

    val gConfig = GoogleCloudConfig.fromConfig(config).get

    assert(gConfig.gcloudBinary === Paths.get(gcloudBinaryPath))
    assert(gConfig.gsutilBinary === Paths.get(gsutilBinaryPath))
    assert(gConfig.projectId === projectId)
    assert(gConfig.clusterId === clusterId)
    assert(gConfig.credentialsFile === Paths.get(credentialsFile))

    import GoogleCloudConfig.Defaults

    assert(gConfig.numWorkers === Defaults.numWorkers)
    assert(gConfig.zone === Defaults.zone)
    assert(gConfig.masterMachineType === Defaults.masterMachineType)
    assert(gConfig.masterBootDiskSize === Defaults.masterBootDiskSize)
    assert(gConfig.workerMachineType === Defaults.workerMachineType)
    assert(gConfig.workerBootDiskSize === Defaults.workerBootDiskSize)
    assert(gConfig.numPreemptibleWorkers === Defaults.numPreemptibleWorkers)
    assert(gConfig.preemptibleWorkerBootDiskSize === Defaults.preemptibleWorkerBootDiskSize)
    assert(gConfig.imageVersion === Defaults.imageVersion)
    assert(gConfig.scopes === Defaults.scopes)
    assert(gConfig.properties === Defaults.properties)
    assert(gConfig.initializationActions === Defaults.initializationActions)
    assert(gConfig.metadata === Defaults.metadata)
    assert(gConfig.maxClusterIdleTime === Defaults.maxClusterIdleTime)
  }

  test("fromConfig - bad input") {
    assert(GoogleCloudConfig.fromConfig(ConfigFactory.empty).isFailure)

    def doTest(s: String): Unit = {
      val config = Try(ConfigFactory.parseString(s))

      assert(config.flatMap(GoogleCloudConfig.fromConfig).isFailure)
    }

    doTest(null) //scalastyle:ignore null
    doTest("")
    doTest("asdsadasd")
    doTest("loamstream { }")
    doTest("loamstream { googlecloud { } }")
  }

  test("fromConfig - no defaults") {
    val confString = s"""loamstream {
        googlecloud {
          gcloudBinary = "$gcloudBinaryPath"
          gsutilBinary = "$gsutilBinaryPath"
          projectId = "$projectId"
          clusterId = "$clusterId"
          credentialsFile = "$credentialsFile"
          numWorkers = $numWorkers
          zone = "$zone"
          masterMachineType = "$masterMachineType"
          masterBootDiskSize = $masterBootDiskSize
          workerMachineType = "$workerMachineType"
          workerBootDiskSize = $workerBootDiskSize
          numPreemptibleWorkers = $numPreemptibleWorkers
          preemptibleWorkerBootDiskSize = $preemptibleWorkerBootDiskSize
          imageVersion = "$imageVersion"
          scopes = "$scopes"
          properties = "$properties"
          initializationActions = "$initializationActions"
          metadata = "$metadata"
          maxClusterIdleTime = "$maxClusterIdleTime"
        }
      }"""

    val config = ConfigFactory.parseString(confString)

    val gConfig = GoogleCloudConfig.fromConfig(config).get

    assert(gConfig.gcloudBinary === Paths.get(gcloudBinaryPath))
    assert(gConfig.gsutilBinary === Paths.get(gsutilBinaryPath))
    assert(gConfig.projectId === projectId)
    assert(gConfig.clusterId === clusterId)
    assert(gConfig.numWorkers === numWorkers)
    assert(gConfig.zone === zone)
    assert(gConfig.masterMachineType === masterMachineType)
    assert(gConfig.masterBootDiskSize === masterBootDiskSize)
    assert(gConfig.workerMachineType === workerMachineType)
    assert(gConfig.workerBootDiskSize === workerBootDiskSize)
    assert(gConfig.numPreemptibleWorkers === numPreemptibleWorkers)
    assert(gConfig.preemptibleWorkerBootDiskSize === preemptibleWorkerBootDiskSize)
    assert(gConfig.imageVersion === imageVersion)
    assert(gConfig.scopes === scopes)
    assert(gConfig.properties === properties)
    assert(gConfig.initializationActions === initializationActions)
    assert(gConfig.metadata === Some(metadata))
    assert(gConfig.maxClusterIdleTime === maxClusterIdleTime)
  }
  
  test("checkMaxClusterIdleTime - bad input") {
    import GoogleCloudConfig.checkMaxClusterIdleTime
    
    assert(checkMaxClusterIdleTime("").isFailure)
    assert(checkMaxClusterIdleTime("  ").isFailure)
    assert(checkMaxClusterIdleTime("asdfg").isFailure)
    assert(checkMaxClusterIdleTime("10mmm").isFailure)
    assert(checkMaxClusterIdleTime("10hhh").isFailure)
    assert(checkMaxClusterIdleTime("10dd").isFailure)
    
    //10 minutes in seconds, minus 1
    assert(checkMaxClusterIdleTime("599s").isFailure)
    assert(checkMaxClusterIdleTime("9m").isFailure)
    assert(checkMaxClusterIdleTime("1m").isFailure)
    assert(checkMaxClusterIdleTime("0m").isFailure)
    assert(checkMaxClusterIdleTime("-1d").isFailure)
    
    //14 days in seconds, plus 1
    assert(checkMaxClusterIdleTime("1209601s").isFailure)
    assert(checkMaxClusterIdleTime("15d").isFailure)
    assert(checkMaxClusterIdleTime("1000d").isFailure)
  }
  
  test("checkMaxClusterIdleTime - good input") {
    import GoogleCloudConfig.checkMaxClusterIdleTime
    
    assert(checkMaxClusterIdleTime("10m").isSuccess)
    assert(checkMaxClusterIdleTime("11m").isSuccess)
    assert(checkMaxClusterIdleTime("700s").isSuccess)
    assert(checkMaxClusterIdleTime("14d").isSuccess)
    assert(checkMaxClusterIdleTime("48h").isSuccess)
  }
}
