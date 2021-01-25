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
  private val numWorkerLocalSsds = 1
  private val zone = "z"
  private val masterMachineType = "mmt"
  private val masterBootDiskSize = 99
  private val workerMachineType = "wmt"
  private val workerBootDiskSize = 17
  private val properties = "p,r,o,p,s"
  private val maxClusterIdleTime = "42h"
  private val region = "region-askdhasf"

  test("fromConfig - defaults used") {
    val confString = s"""loamstream {
        googlecloud {
          gcloudBinary = "$gcloudBinaryPath"
          gsutilBinary = "$gsutilBinaryPath"
          projectId = "$projectId"
          clusterId = "$clusterId"
          credentialsFile = "$credentialsFile"
          region = "${region}"
        }
      }"""

    val config = ConfigFactory.parseString(confString)

    val gConfig = GoogleCloudConfig.fromConfig(config).get

    assert(gConfig.gcloudBinary === Paths.get(gcloudBinaryPath))
    assert(gConfig.gsutilBinary === Paths.get(gsutilBinaryPath))
    assert(gConfig.projectId === projectId)
    assert(gConfig.clusterId === clusterId)
    assert(gConfig.credentialsFile === Paths.get(credentialsFile))
    assert(gConfig.region === region)

    import GoogleCloudConfig.Defaults

    assert(gConfig.defaultClusterConfig === Defaults.clusterConfig)
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
          region = "${region}"
          defaultClusterConfig {
            numWorkers = $numWorkers
            zone = "$zone"
            masterMachineType = "$masterMachineType"
            masterBootDiskSize = $masterBootDiskSize
            workerMachineType = "$workerMachineType"
            workerBootDiskSize = $workerBootDiskSize
            numPreemptibleWorkers = $numPreemptibleWorkers
            preemptibleWorkerBootDiskSize = $preemptibleWorkerBootDiskSize
            numWorkerLocalSsds = $numWorkerLocalSsds
            properties = "$properties"
            maxClusterIdleTime = "$maxClusterIdleTime"
          }
        }
      }"""

    val config = ConfigFactory.parseString(confString)

    val gConfig = GoogleCloudConfig.fromConfig(config).get

    assert(gConfig.gcloudBinary === Paths.get(gcloudBinaryPath))
    assert(gConfig.gsutilBinary === Paths.get(gsutilBinaryPath))
    assert(gConfig.projectId === projectId)
    assert(gConfig.clusterId === clusterId)
    assert(gConfig.credentialsFile === Paths.get(credentialsFile))
    assert(gConfig.region === region)
    assert(gConfig.defaultClusterConfig.numWorkers === numWorkers)
    assert(gConfig.defaultClusterConfig.zone === zone)
    assert(gConfig.defaultClusterConfig.masterMachineType === masterMachineType)
    assert(gConfig.defaultClusterConfig.masterBootDiskSize === masterBootDiskSize)
    assert(gConfig.defaultClusterConfig.workerMachineType === workerMachineType)
    assert(gConfig.defaultClusterConfig.workerBootDiskSize === workerBootDiskSize)
    assert(gConfig.defaultClusterConfig.numPreemptibleWorkers === numPreemptibleWorkers)
    assert(gConfig.defaultClusterConfig.preemptibleWorkerBootDiskSize === preemptibleWorkerBootDiskSize)
    assert(gConfig.defaultClusterConfig.numWorkerLocalSsds === numWorkerLocalSsds)
    assert(gConfig.defaultClusterConfig.properties === properties)
    assert(gConfig.defaultClusterConfig.maxClusterIdleTime === maxClusterIdleTime)
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
