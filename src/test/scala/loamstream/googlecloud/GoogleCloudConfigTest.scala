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
  private val binaryPath = "/path/to/gcloud"
  private val projectId = "pid"
  private val clusterId = "cid"
  private val credential = "/path/to/credential"
  private val numWorkers = 42
  private val zone = "z"
  private val masterMachineType = "mmt"
  private val masterBootDiskSize = 99
  private val workerMachineType = "wmt"
  private val workerBootDiskSize = 17
  private val imageVersion = "iv"
  private val scopes = "ses"
  
  test("fromConfig - defaults used") {
    val confString = s"""loamstream {
        googlecloud {
          gcloudBinary = "$binaryPath"
          projectId = "$projectId"
          clusterId = "$clusterId"
          credential = "$credential"
          numWorkers = $numWorkers
        }
      }"""
          
    val config = ConfigFactory.parseString(confString)
    
    val gConfig = GoogleCloudConfig.fromConfig(config).get
    
    assert(gConfig.gcloudBinary === Paths.get(binaryPath))
    assert(gConfig.projectId === projectId)
    assert(gConfig.clusterId === clusterId)
    assert(gConfig.credential === Paths.get(credential))
    assert(gConfig.numWorkers === numWorkers)
    
    import GoogleCloudConfig.Defaults
    
    assert(gConfig.zone === Defaults.zone)
    assert(gConfig.masterMachineType === Defaults.masterMachineType)
    assert(gConfig.masterBootDiskSize === Defaults.masterBootDiskSize)
    assert(gConfig.workerMachineType === Defaults.workerMachineType)
    assert(gConfig.workerBootDiskSize === Defaults.workerBootDiskSize)
    assert(gConfig.imageVersion === Defaults.imageVersion)
    assert(gConfig.scopes === Defaults.scopes)
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
          gcloudBinary = "$binaryPath"
          projectId = "$projectId"
          clusterId = "$clusterId"
          credential = "$credential"
          numWorkers = $numWorkers
          zone = "$zone"
          masterMachineType = "$masterMachineType"
          masterBootDiskSize = $masterBootDiskSize
          workerMachineType = "$workerMachineType"
          workerBootDiskSize = $workerBootDiskSize
          imageVersion = "$imageVersion"
          scopes = "$scopes"
        }
      }"""
          
    val config = ConfigFactory.parseString(confString)
    
    val gConfig = GoogleCloudConfig.fromConfig(config).get
    
    assert(gConfig.gcloudBinary === Paths.get(binaryPath))
    assert(gConfig.projectId === projectId)
    assert(gConfig.clusterId === clusterId)
    assert(gConfig.numWorkers === numWorkers)
    assert(gConfig.zone === zone)
    assert(gConfig.masterMachineType === masterMachineType)
    assert(gConfig.masterBootDiskSize === masterBootDiskSize)
    assert(gConfig.workerMachineType === workerMachineType)
    assert(gConfig.workerBootDiskSize === workerBootDiskSize)
    assert(gConfig.imageVersion === imageVersion)
    assert(gConfig.scopes === scopes)
  }
}