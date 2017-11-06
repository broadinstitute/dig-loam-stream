package loamstream.googlecloud

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import java.nio.file.Paths
import scala.util.Try

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
  }
}
