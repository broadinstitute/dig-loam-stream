package loamstream.googlecloud

import org.scalatest.FunSuite
import java.net.URI
import loamstream.IntegrationTestHelpers
import org.scalactic.source.Position.apply

/**
 * @author clint
 * Jun 25, 2019
 */
final class GcsClientAndDriverTest extends FunSuite {
  import IntegrationTestHelpers.path
  
  private val bucketName = "loamstream"
  private val testDataDir = "integration-tests"
  /*
   * googlecloud {
    gcloudBinary = "/humgen/diabetes/users/dig/loamstream/google-cloud-sdk/bin/gcloud"
    gsutilBinary = "/humgen/diabetes/users/dig/loamstream/google-cloud-sdk/bin/gsutil"
    imageVersion = "1.2-deb9"
    metadata = "MINICONDA_VERSION=4.4.10,JAR=gs://hail-common/builds/0.2/jars/hail-0.2-e08cc2a17c4a-Spark-2.2.0.jar,ZIP=gs://hail-common/builds/0.2/python/hail-0.2-e08cc2a17c4a.zip"
    initializationActions = "gs://dataproc-initialization-actions/conda/bootstrap-conda.sh,gs://hail-common/cloudtools/init_notebook1.py"
    projectId = "broadinstitute.com:cmi-gce-01"
    clusterId = "loamstream-integration-tests"
    credentialsFile = "/humgen/diabetes/users/dig/google_credential.json"
    masterMachineType = "n1-standard-1"
    workerMachineType = "n1-standard-1"
    numWorkers = 2
    numPreemptibleWorkers = 0
   */
  
  //private val credentialFile = path("/humgen/diabetes/users/dig/google_credential.json")
  private val credentialFile = path("/home/clint/workspace/google_credential.json")
  
  test("blobsAt") {
    val driver = new GcsDriver(credentialFile)
    
    val dirName = "foo/bar/"
    
    val k0 = "foo/bar/baz"
    val k1 = "foo/bar/blerg"
    val k2 = "foo/bar/zerg"
    
    deleteTestDataAndThen(driver, "blobsAt") { (subKey, uriOf) =>
      driver.put(uriOf(k0), "asdf")
      driver.put(uriOf(k1), "xyz")
      driver.put(uriOf(k2), "abc")
      
      {
        val blobs = driver.blobsAt(uriOf("foo/"))
        
        assert(blobs.map(_.name).toSet === Set(subKey(k0), subKey(k1), subKey(k2)))
      }
      
      {
        val blobs = driver.blobsAt(uriOf(dirName))
        
        assert(blobs.map(_.name).toSet === Set(subKey(k0), subKey(k1), subKey(k2)))
      }
      
      {
        val blobs = driver.blobsAt(uriOf(k0))
        
        assert(blobs.map(_.name).toSet === Set(subKey(k0)))
      }
      
      {
        val blobs = driver.blobsAt(uriOf("foo/bar/baz/blip/zip/trip"))
        
        assert(blobs.isEmpty)
      }
    }
  }
  
  test("isPresent") {
    val driver = new GcsDriver(credentialFile)
    
    val client = new GcsClient(driver)
    
    val dirName = "foo/bar/"
    
    val k0 = s"${dirName}baz"
    val k1 = s"${dirName}blerg"
    val k2 = s"${dirName}zerg"
    
    val emptyDirName = s"${dirName}empty/" 
    
    deleteTestDataAndThen(driver, "isPresent") { (_, uriOf) =>
      driver.put(uriOf(k0), "asdf")
      driver.put(uriOf(k1), "xyz")
      driver.put(uriOf(k2), "abc")
      
      driver.put(uriOf(emptyDirName), "")
      
      assert(client.isPresent(uriOf(k0)))
      assert(client.isPresent(uriOf(k1)))
      assert(client.isPresent(uriOf(k2)))
      
      assert(client.isPresent(uriOf(dirName)))
      assert(client.isPresent(uriOf(emptyDirName)))
      
      assert(client.isPresent(uriOf("foo/bar/baz/blip/zip/trip")) === false)
    }
  }
  
  private def deleteTestDataAndThen[A](
      driver: GcsDriver, 
      testName: String)(f: (String => String, String => URI) => A) {
    val testSubDir = s"${testDataDir}/${testName}"
    
    driver.deleteWithPrefix(bucketName, testSubDir)
    
    def subKey(key: String): String = s"${testSubDir}/${key}"
  
    def uriOf(key: String): URI = URI.create(s"gs://${bucketName}/${subKey(key)}")
    
    f(subKey, uriOf)
  }
}
